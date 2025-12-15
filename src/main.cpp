#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
using namespace std;

int serverFD;
vector<int> clients;
map<int,struct sockaddr_in> clientINFO;

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

string encodeRESP(vector<string> str , bool isArr = false) {
  string res = "";
  if(isArr) {
    res = "*"+to_string(int(str.size())-1)+"\r\n";
  }
  for(int i=1 ;i<str.size() ;i++) {
    res+="$"+to_string(int(str[i].size()))+"\r\n"+str[i]+"\r\n";
  }

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
        const char* response = NULL;
        transform(tokens[0].begin(), tokens[0].end(), tokens[0].begin(),  
        [](unsigned char c){ return std::toupper(c);});

        if(bytesRead > 0) {
          if(tokens[0] == "PING") {
            response = "+PONG\r\n";
            send(currFD, response , strlen(response) , 0); 
          }
          else if(tokens[0] == "ECHO") {
            response = encodeRESP(tokens).c_str();
            send(currFD, response , strlen(response) , 0);
          }
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
