jedis
=====


Jedis客户端不是线程安全的，主要症结在于接收数据的时候是线程不安全的，多线程的情况下，多线程对一个socket的inputstream进行read，而且每个线程是read一段数据，解析一段数据，这样在多线程的情况下，多次连续的read肯定不能保证不被其他线程打断，在多线程的情况下，是一定被打断的，这个是早就已知的问题。


平时使用中，如果是多线程要使用连接池，也就是多线程的环境下，使用的是不同的连接。这样大规模多线程，必定会加剧客户端socket fd和服务端socket fd的占用。而且每个连接发送的数据，十分少，也就是说服务端解析请求的很轻易，协议比较轻，适合一次传送多次请求，原有模式每次请求就write请求道服务端是比较浪费的。


对于客户端的read过程，jedis的read只是阻塞傻等，多线程环境下只是每个连接返回这个请求的响应。这样并未充分利用cpu。


项目的优势：
-------------


基于nio模式，只占用有限固定的网络连接，写请求会排队，注册写事件，在网络可写的情况下，一次把请求数据全部推送到服务端，也会注册读事件，大并发的情况下，响应会成批的返回，一次全部read下来，然后再去分配给不同的请求。会给予用户更多的控制，采用全异步模式，操作之后返回给用户furture，让用户去决定什么时候关心请求结果。
	

