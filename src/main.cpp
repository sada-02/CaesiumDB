#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <set>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>
#include <optional>
using namespace std;

int serverFD;
vector<int> clients;
map<int,struct sockaddr_in> clientINFO;

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

struct List{
  ListNode* root;
  int size;

  List(string str="") {
    root = new ListNode(str);
    size = 1;
  }
};

map<string,metaData> DATA;
map<string,List> LISTS;

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

string encodeRESP(const vector<string>& str , bool isArr = false) {
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
    if(LISTS.find(tokens[1]) == LISTS.end()) {
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
  return size;
}

string handlePOP(string& str) {
  if(LISTS.find(str)==LISTS.end() || LISTS[str].size()==0) return "";
  ListNode* temp = LISTS[str].root;
  LISTS[str].root = LISTS[str].root->next;
  string res = temp->key;
  delete temp;

  return res;
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

    select(maxFD + 1 , &readFDs , NULL , NULL , NULL);

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

      if(FD_ISSET(currFD , &readFDs)) {
        char buffer[1024];
        int bytesRead = recv(currFD , buffer , sizeof(buffer) , 0);
        vector<string> tokens = RESPparser(buffer);
        string response = "";
        upperCase(tokens[0]);

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
            string element = handlePOP(tokens[1]);
            if(element.size() == 0) {
              response = "$-1\r\n";
            }
            else {
              response = encodeRESP(vector<string> {"GARBAGE" , element});
            }
          }

          send(currFD, response.c_str() , response.size() , 0);
        } 
        else {
          close(currFD);
          clients.erase(clients.begin() + i);
          clientINFO.erase(currFD);
        }
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
  
  eventLoop();
  close(serverFD);

  return 0;
}
