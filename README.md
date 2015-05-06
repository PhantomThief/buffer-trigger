buffer-trigger [![Build Status](https://travis-ci.org/PhantomThief/buffer-trigger.svg)](https://travis-ci.org/PhantomThief/buffer-trigger) [![Coverage Status](https://coveralls.io/repos/PhantomThief/buffer-trigger/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/buffer-trigger?branch=master)
=======================

A local data buffer with customizable data trigger

* multiple data trigger
* buffer container can be customized
* jdk1.8 only

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>buffer-trigger</artifactId>
    <version>0.2.1</version>
</dependency>
```

## Usage

```Java

BufferTrigger<String> buffer = SimpleBufferTrigger.<String, List<String>> newBuilder() //
        .on(3, TimeUnit.SECONDS, 1) //
        .on(2, TimeUnit.SECONDS, 10) //
        .on(1, TimeUnit.SECONDS, 10000) //
        .consumer(this::out) //
        .setContainer(() -> Collections.synchronizedList(new ArrayList<String>()),
                List::add) //
        .build();
        
Random rnd = new Random();
for (int i = 0; i <= 100; i++) {
    String e = i + "";
    System.out.println("enqueue:" + i);
    buffer.enqueue(e);
    Thread.sleep(rnd.nextInt(1000));
}
    
```

## Special Thanks

perlmonk with his great team gives me a huge help.
(https://github.com/aloha-app/thrift-client-pool-java)
