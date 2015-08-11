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
    <version>0.2.5</version>
</dependency>
```

## Usage

```Java

// declare
BufferTrigger<String> buffer = SimpleBufferTrigger.<String, Set<String>> newBuilder() //
        .on(3, TimeUnit.SECONDS, 1) //
        .on(2, TimeUnit.SECONDS, 10) //
        .on(1, TimeUnit.SECONDS, 10000) //
        .consumer(this::out) //
        .setContainer(ConcurrentSkipListSet::new, Set::add) // default is Collections.synchronizedSet(new HashSet())
        .build();
        
// enqueue
buffer.enqueue("i'm ok");

// consumer declare
private void out(Collection<String> set) {
	set.forEach(System.out::println);
}

// simple queue ver.
BufferTrigger<String> buffer = SimpleBufferTrigger.newBlockingQueueBuilder(3, this::out)
                .on(5, TimeUnit.SECONDS, 1) //
                .build();
    
```

## Special Thanks

perlmonk with his great team gives me a huge help.
(https://github.com/aloha-app)
