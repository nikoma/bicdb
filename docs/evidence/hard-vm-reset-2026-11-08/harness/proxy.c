/* Test-guest TCP relay; leaves the exact server binary and protocol unchanged. */
#include <arpa/inet.h>
#include <errno.h>
#include <poll.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
static int transfer(int from, int to) {
 char buf[32768]; ssize_t n=recv(from,buf,sizeof(buf),0);
 if(n<=0)return -1;
 for(ssize_t at=0;at<n;) { ssize_t sent=send(to,buf+at,n-at,MSG_NOSIGNAL); if(sent<0&&errno==EINTR)continue; if(sent<=0)return -1;at+=sent; }
 return 0;
}
int main(int argc,char **argv) {
 if(argc!=3)return 2;
 signal(SIGCHLD,SIG_IGN);
 int listener=socket(AF_INET,SOCK_STREAM,0),one=1;
 setsockopt(listener,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one));
 struct sockaddr_in local={.sin_family=AF_INET,.sin_port=htons(atoi(argv[1])),.sin_addr.s_addr=htonl(INADDR_ANY)};
 if(bind(listener,(void*)&local,sizeof(local))||listen(listener,64))return 3;
 for(;;) {
  int client=accept(listener,0,0);if(client<0)continue;
  pid_t pid=fork(); if(pid<0){close(client);continue;} if(pid>0){close(client);continue;}
  close(listener);int server=socket(AF_INET,SOCK_STREAM,0);
  setsockopt(client,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
  setsockopt(server,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
  struct sockaddr_in target={.sin_family=AF_INET,.sin_port=htons(atoi(argv[2])),.sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
  if(connect(server,(void*)&target,sizeof(target)))_exit(4);
  struct pollfd fds[2]={{.fd=client,.events=POLLIN},{.fd=server,.events=POLLIN}};
  for(;;) { if(poll(fds,2,-1)<0){if(errno==EINTR)continue;break;} if(fds[0].revents&&transfer(client,server))break;if(fds[1].revents&&transfer(server,client))break; }
  close(client);close(server);_exit(0);
 }
}
