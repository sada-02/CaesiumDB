#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <deque>
#include <map>
#include <set>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>
#include <optional>
#include <climits>
using namespace std;

int serverFD;
vector<int> clients;
map<int,struct sockaddr_in> clientINFO;
pair<long long,long long> lastSTREAMID;

string encodeRESP(const vector<string>& str , bool isArr = false);

struct metaData {
  string DATA;
  optional<chrono::steady_clock::time_point> expiryTime;
};

struct ListNode {
  ListNode* next;
  string key;

  ListNode(string str = "") {
    next = NULL;
    key = str;
  }
};

struct blocklist{
  int clientFD;
  chrono::steady_clock::time_point timeout;
  bool indef;
  blocklist* next;

  blocklist(int id , chrono::steady_clock::time_point t , bool flag = false) {
    clientFD = id;
    timeout = t;
    indef = flag;
  }
};

struct List{
  ListNode* root;
  int size;
  blocklist* blocks;

  List(string str="") {
    root = new ListNode(str);
    size = 1;
    blocks = nullptr;
  }

  void insert(int cFD , chrono::steady_clock::time_point t , bool flag = false) {
    if(!blocks) {
      blocks = new blocklist(cFD , t , flag);
    }
    else {
      blocklist* temp = blocks;
      while(temp->next) temp = temp->next;
      temp->next = new blocklist(cFD , t , flag);
    }
  }

  void handleREQ(const string& str) {
    if(!blocks) return ;

    blocklist* temp = blocks;
    string response = "*-1\r\n";
    while(temp && !temp->indef && chrono::steady_clock::now() > temp->timeout) {
      send(temp->clientFD,response.c_str(),response.size(),0);
      blocklist* prev = temp;
      temp = temp->next;
      delete prev;
    }
    if(!temp) return;

    ListNode* ele = root;
    root = root->next;
    size--;
    response = encodeRESP(vector<string> {"GARBAGE" , str , ele->key}, true);
    delete ele;
    send(temp->clientFD,response.c_str(),response.size(),0);
    blocks = temp->next;
    delete temp;
  }
};

map<string,metaData> DATA;
map<string,List> LISTS;
map<string,map<long long,map<long long,map<string,string>>>> STREAM;

vector<string> RESPparser(const char* str) {
  int n = strlen(str);
  vector<string> tokens ;
  string num = "";
  bool takeNum = false;
  for(int i=0 ;i<n ;i++) {
    if(takeNum) {
      if((str[i]>='0' && str[i]<='9')) {
        num+=str[i];
      }
      else {
        takeNum = false;
        num = "";  
        i+=2;
        string inp = "";

        while(i<n-1 && str[i] != '\r' && str[i+1] != '\n') {
          inp+=str[i];
          i++;
        }
        i--;
        tokens.push_back(inp);
      }
    }
    if(str[i] == '$') {
      takeNum = true;
    }
  }

  return tokens;
}

string encodeRESP(const vector<string>& str , bool isArr) {
  string res = "";
  if(isArr) {
    res = "*"+to_string(int(str.size())-1)+"\r\n";
  }
  for(int i=1 ;i<str.size() ;i++) {
    res+="$"+to_string(int(str[i].size()))+"\r\n"+str[i]+"\r\n";
  }

  return res;
}

string encodeRESPint(int val) {
  return ":"+to_string(val)+"\r\n";
}

string encodeRESPsimpleSTR(const string& str) {
  return "+"+str+"\r\n";
}

string encodeRESPsimpleERR(const string& str) {
  return "-"+str+"\r\n";
}

void upperCase(string& str) {
  transform(str.begin(), str.end(), str.begin(),[](unsigned char c){ return std::toupper(c);});
  return;
}

void handleSET(vector<string>& tokens) {
  DATA[tokens[1]].DATA = tokens[2];
  if(tokens.size()>3) {
    for(int i=3 ;i<tokens.size() ;i+=2) {
      upperCase(tokens[i]);
      if(tokens[i] == "EX") {
        int time = stoi(tokens[i+1]);
        DATA[tokens[1]].expiryTime = chrono::steady_clock::now() + chrono::seconds(time);
      }
      else if(tokens[i] == "PX") {
        int time = stoi(tokens[i+1]);
        DATA[tokens[1]].expiryTime = chrono::steady_clock::now() + chrono::milliseconds(time);
      }
    }
  }
}

void handleGET(const string& str) {
  if(DATA.find(str) != DATA.end() && DATA[str].expiryTime.has_value()) {
    if(chrono::steady_clock::now() >= DATA[str].expiryTime) {
      DATA.erase(DATA.find(str));
    }
  }
}

int handlePUSH(const vector<string>& tokens , bool isAppend = true) {
  int size = -1;
  for(int i=2 ;i<tokens.size() ;i++) {
    if(LISTS.find(tokens[1]) == LISTS.end() || LISTS[tokens[1]].root == nullptr) {
      LISTS[tokens[1]].root = new ListNode(tokens[i]);
      LISTS[tokens[1]].size = 1;
      size = 1;
    }
    else {
      ListNode* temp = LISTS[tokens[1]].root;

      if(isAppend) {
        while(temp->next) {
          temp = temp->next;
        }
        temp->next = new ListNode(tokens[i]);
      }
      else {
        ListNode* newNode = new ListNode(tokens[i]);
        newNode->next = temp;
        LISTS[tokens[1]].root = newNode;
      }
      size = ++LISTS[tokens[1]].size;
    }
  }

  while(LISTS[tokens[1]].root && LISTS[tokens[1]].blocks) {
    LISTS[tokens[1]].handleREQ(tokens[1]);
  }

  return size;
}

vector<string> handlePOP(string& str , int numEle=1) {
  if(LISTS.find(str)==LISTS.end() || LISTS[str].size==0) return vector<string> {};
  vector<string> res;
  res.push_back("GARBAGE");

  for(int i=0 ;i<numEle && LISTS[str].root ;i++) {
    ListNode* temp = LISTS[str].root;
    LISTS[str].root = LISTS[str].root->next;
    res.push_back(temp->key);
    delete temp;
  }

  return res;
}

void checkBlockedTimeouts() {
  auto now = chrono::steady_clock::now();
  string response = "*-1\r\n";
  
  for(auto& l : LISTS) {
    if(!l.second.blocks) continue;
    
    blocklist* temp = l.second.blocks;
    blocklist* prev = nullptr;
    
    while(temp) {
      if(!temp->indef && now > temp->timeout) {
        send(temp->clientFD, response.c_str(), response.size(), 0);
        
        if(prev) {
          prev->next = temp->next;
          delete temp;
          temp = prev->next;
        } 
        else {
          l.second.blocks = temp->next;
          delete temp;
          temp = l.second.blocks;
        }
      } 
      else {
        prev = temp;
        temp = temp->next;
      }
    }
  }
}

void completeID(vector<string>& tokens) {
  if(tokens[2] == "*") {
    long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count();
    if(ms == lastSTREAMID.first) {
      lastSTREAMID.second++;
    }
    else {
      lastSTREAMID.first = ms;
      lastSTREAMID.second = 0;
    }
    tokens[2] = to_string(lastSTREAMID.first)+"-"+to_string(lastSTREAMID.second);
  }
  else {
    vector<string> seqNum;
    stringstream ID(tokens[2]);
    string str;
    while(getline(ID,str,'-')) seqNum.push_back(str);
    
    if(seqNum[1] == "*") {
      if(lastSTREAMID.first == stoll(seqNum[0])) {
        lastSTREAMID.second = lastSTREAMID.second+1;
      }
      else {
        lastSTREAMID.first = stoll(seqNum[0]);
        lastSTREAMID.second = 0;
      }

      tokens[2] = to_string(lastSTREAMID.first)+"-"+to_string(lastSTREAMID.second);
    }
  }
}

bool checkSTREAMID(string& id) {
  if(id == "*") return true;
  
  vector<string> tokens;
  stringstream ID(id);
  string str;
  while(getline(ID,str,'-')) tokens.push_back(str);
  if(tokens[1] == "*") return true;

  if(stoll(tokens[0])>lastSTREAMID.first) {
    lastSTREAMID = make_pair(stoll(tokens[0]) , stoll(tokens[1]));
    return true;
  }
  else if(stoll(tokens[0]) == lastSTREAMID.first) {
    if(stoll(tokens[1]) > lastSTREAMID.second) {
      lastSTREAMID = make_pair(stoll(tokens[0]) , stoll(tokens[1]));
      return true; 
    }
  }

  return false;
}

string handleXRANGE(vector<string>& tokens) {
  long long startMS = -1 , startSEQ = 0 , endMS = -1 , endSEQ = LLONG_MAX;
  stringstream inp(tokens[2]);
  string str;
  vector<string> seqNum;
  
  if(tokens[2] == "-") {
    startMS = 0;
  }
  else {
    while(getline(inp,str,'-')) seqNum.push_back(str);
    startMS = stoll(seqNum[0]);
    startSEQ = stoll(seqNum[1]);
  }

  seqNum.clear();
  
  if(tokens[3] == "+") {
    endMS = LLONG_MAX;
  }
  else {
    inp.str(tokens[3]);
    inp.clear();
    while(getline(inp,str,'-')) seqNum.push_back(str);
    endMS = stoll(seqNum[0]);
    endSEQ = stoll(seqNum[1]); 
  }
  
  string result = "";
  int Count = 0;
  
  for(auto& m : STREAM[tokens[1]]) {
    long long ms = m.first;
    
    if(ms < startMS) continue;
    
    if(ms > endMS) break;
    
    for(auto& s : m.second) {
      long long seq = s.first;
      
      bool inRange = false;
      if(ms == startMS && ms == endMS) {
        inRange = (seq >= startSEQ && seq <= endSEQ);
      }
      else if(ms == startMS) {
        inRange = (seq >= startSEQ);
      }
      else if(ms == endMS) {
        inRange = (seq <= endSEQ);
      }
      else {
        inRange = true;
      }
      
      if(inRange) {
        Count++;
        string id = to_string(ms) + "-" + to_string(seq);
        result += "*2\r\n"; 
        result += "$" + to_string(id.size()) + "\r\n" + id + "\r\n";
        
        int numKeys = s.second.size() * 2; 
        result += "*" + to_string(numKeys) + "\r\n";
        
        for(auto& kv : s.second) {
          result += "$" + to_string(kv.first.size()) + "\r\n" + kv.first + "\r\n";
          result += "$" + to_string(kv.second.size()) + "\r\n" + kv.second + "\r\n";
        }
      }
    }
  }

  return "*" + to_string(Count) + "\r\n" + result;
}

string handleTYPE(const vector<string>& tokens) {
  if(DATA.find(tokens[1]) != DATA.end()) {
    return encodeRESPsimpleSTR("string");
  }
  else if(STREAM.find(tokens[1]) != STREAM.end()) {
    return encodeRESPsimpleSTR("stream");
  }
  else {
    return encodeRESPsimpleSTR("none");
  }
}

void eventLoop() {
  while(true) {
    fd_set readFDs;
    FD_ZERO(&readFDs);
    FD_SET(serverFD,&readFDs);
    int maxFD = serverFD;

    for(int id : clients) {
      FD_SET(id,&readFDs);
      maxFD = max(id,maxFD);
    }

    auto now = chrono::steady_clock::now();
    chrono::steady_clock::time_point leastTime;
    bool hasTimeout = false;
    
    for(const auto& l : LISTS) {
      blocklist* temp = l.second.blocks;
      while(temp) {
        if(!temp->indef) {
          if(!hasTimeout || temp->timeout < leastTime) {
            leastTime = temp->timeout;
            hasTimeout = true;
          }
        }
        temp = temp->next;
      }
    }
    
    struct timeval timeout;
    if(hasTimeout) {
      auto remTime = leastTime - now;
      if(remTime.count() <= 0) {
        timeout.tv_sec = 0;
        timeout.tv_usec = 0;
      } 
      else {
        auto s = chrono::duration_cast<chrono::seconds>(remTime);
        auto ms = chrono::duration_cast<chrono::microseconds>(remTime - s);
        timeout.tv_sec = s.count();
        timeout.tv_usec = ms.count();
      }
    } 
    else {
      timeout.tv_sec = 1;
      timeout.tv_usec = 0;
    }
    timeout.tv_usec += 20000;
    
    select(maxFD + 1 , &readFDs , NULL , NULL , &timeout);
    
    checkBlockedTimeouts();

    if(FD_ISSET(serverFD,&readFDs)) {
      struct sockaddr_in client_addr;
      int client_addr_len = sizeof(client_addr);
      int clientFD = accept(serverFD, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
      clientINFO[clientFD] = client_addr;
      clients.push_back(clientFD);
      cout << "Client connected\n";
    }
    
    for(int i = clients.size() - 1; i >= 0; i--) {
      int currFD = clients[i];
      bool sendResponse = true;

      if(FD_ISSET(currFD , &readFDs)) {
        char buffer[1024];
        int bytesRead = recv(currFD , buffer , sizeof(buffer) , 0);
        if(bytesRead > 0) {
          buffer[bytesRead] = '\0';
        }
        vector<string> tokens = RESPparser(buffer);
        string response = "";
        upperCase(tokens[0]);

        cout << "Tokens[" << tokens.size() << "]: ";
        for(const auto& t : tokens) {
          cout << "\"" << t << "\" ";
        }
        cout << endl;

        if(bytesRead > 0) {
          if(tokens[0] == "PING") {
            response = "+PONG\r\n";
          }
          else if(tokens[0] == "ECHO") {
            response = encodeRESP(tokens);
          }
          else if(tokens[0] == "SET") {
            response = "+OK\r\n";
            handleSET(tokens);
          }
          else if(tokens[0] == "GET") {
            handleGET(tokens[1]);
            if(DATA.find(tokens[1]) == DATA.end()) {
              response = "$-1\r\n";
            }
            else {
              response = encodeRESP(vector<string> {"GARBAGE" , DATA[tokens[1]].DATA});
            }
          }
          else if(tokens[0] == "RPUSH" || tokens[0] == "LPUSH") {
            int lsize = handlePUSH(tokens , tokens[0][0] == 'R');
            if(lsize>0) {
              response = encodeRESPint(lsize);
            }
          }
          else if(tokens[0] == "LRANGE") {
            int startIDX = stoi(tokens[2]) , endIDX = stoi(tokens[3]);
            if(LISTS.find(tokens[1]) == LISTS.end()) {
              response = "*0\r\n";
            }
            else {
              if(startIDX<0) {
                startIDX = LISTS[tokens[1]].size + startIDX;
              }
              if(startIDX<0) {
                startIDX = 0;
              }

              if(endIDX<0) {
                endIDX = LISTS[tokens[1]].size + endIDX;
              }
              if(endIDX<0) {
                endIDX = 0;
              }

              if(LISTS[tokens[1]].size < startIDX || startIDX > endIDX) {
                response = "*0\r\n";
              }
              else {
                vector<string> keys;
                keys.push_back("GARBAGE");
                int i=0;
                ListNode* temp = LISTS[tokens[1]].root;
                while(i<startIDX) {
                  i++;
                  temp = temp->next;
                }
                endIDX = min(endIDX , LISTS[tokens[1]].size-1);

                while(temp && i<=endIDX) {
                  i++;
                  keys.push_back(temp->key);
                  temp = temp->next;
                }

                response = encodeRESP(keys , true);
              }
            }
          }
          else if(tokens[0] == "LLEN") {
            int lsize = 0;
            if(LISTS.find(tokens[1]) != LISTS.end()) lsize = LISTS[tokens[1]].size;
            response = encodeRESPint(lsize);
          }
          else if(tokens[0] == "LPOP") {
            int numEle = 1;
            if(tokens.size() > 2) numEle = stoi(tokens[2]);

            vector<string> element = handlePOP(tokens[1],numEle);
            if(element.size() == 0) {
              response = "$-1\r\n";
            }
            else {
              response = encodeRESP(element , tokens.size()>2);
            }
          }
          else if(tokens[0] == "BLPOP") {
            if(LISTS.find(tokens[1]) == LISTS.end() || LISTS[tokens[1]].size == 0) {
              if(LISTS.find(tokens[1]) == LISTS.end()) {
                LISTS[tokens[1]].root = nullptr;
                LISTS[tokens[1]].size = 0;
                LISTS[tokens[1]].blocks = nullptr;
              }
              auto timeoutDuration = chrono::milliseconds(static_cast<long long>(stod(tokens[2]) * 1000));
              LISTS[tokens[1]].insert(currFD, chrono::steady_clock::now() + 
              timeoutDuration, tokens[2]=="0");
              sendResponse = false;
            }
            else {
              ListNode* ele = LISTS[tokens[1]].root;
              LISTS[tokens[1]].root = LISTS[tokens[1]].root->next;
              LISTS[tokens[1]].size--;
              response = encodeRESP(vector<string> {"GARBAGE" , tokens[1] , ele->key}, true);
              delete ele;
            }
          }
          else if(tokens[0] == "XADD") {
            if(tokens[2] == "0-0") {
              response = encodeRESPsimpleERR("ERR The ID specified in XADD must be greater than 0-0");
            }
            else if(!checkSTREAMID(tokens[2])) {
              response = encodeRESPsimpleERR("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            else {
              completeID(tokens);

              for(int i=3 ;i<tokens.size() ;i+=2) {
                STREAM[tokens[1]][lastSTREAMID.first][lastSTREAMID.second][tokens[i]] = tokens[i+1]; 
              }
              response = encodeRESP(vector<string> {"GARBAGE" , tokens[2]});
            }
          }
          else if(tokens[0] == "XRANGE") {
            response = handleXRANGE(tokens);
          }
          else if(tokens[0] == "XREAD") {
            upperCase(tokens[1]);
            if(tokens[1] == "STREAMS") {
                stringstream inp(tokens[3]);
                string str;
                vector<string> seqNum;
                while(getline(inp,str,'-')) seqNum.push_back(str);
                long long ms = stoll(seqNum[0]) , seq = stoll(seqNum[1]);

                response = "*1\r\n*2\r\n$"+to_string(tokens[2].size())+"\r\n"+tokens[2]+"\r\n*1\r\n";
                response += "*2\r\n$"+to_string(tokens[3].size())+"\r\n"+tokens[3]+"\r\n*";
                response += to_string(2*STREAM[tokens[2]][ms][seq].size())+"\r\n";

                for(const auto& kv : STREAM[tokens[2]][ms][seq]) {
                  response += "$"+to_string(kv.first.size())+"\r\n"+kv.first+"\r\n";
                  response += "$"+to_string(kv.second.size())+"\r\n"+kv.second+"\r\n";
                }
            }
          }
          else if(tokens[0] == "TYPE") {
            response = handleTYPE(tokens);
          }

        if(sendResponse) send(currFD, response.c_str() , response.size() , 0);
        } 
        else {
          close(currFD);
          clients.erase(clients.begin() + i);
          clientINFO.erase(currFD);
        }

        tokens.clear();
      }
    }
  }
}

int main(int argc, char **argv) {
  cout << unitbuf;
  cerr << unitbuf;
  
  serverFD = socket(AF_INET, SOCK_STREAM, 0);
  if (serverFD < 0) {
   cerr << "Failed to create server socket\n";
   return 1;
  }
  
  int reuse = 1;
  if (setsockopt(serverFD, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(serverFD, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(serverFD, connection_backlog) != 0) {
    cerr << "listen failed\n";
    return 1;
  }

  lastSTREAMID = {0,0};
  
  eventLoop();
  close(serverFD);

  return 0;
}
