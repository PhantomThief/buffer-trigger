buffer-trigger [![Build Status](https://travis-ci.org/PhantomThief/buffer-trigger.svg)](https://travis-ci.org/PhantomThief/buffer-trigger) [![Coverage Status](https://coveralls.io/repos/PhantomThief/buffer-trigger/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/buffer-trigger?branch=master)
=======================

A local data buffer with customizable data trigger

* customize trigger strategy
* buffer container can be customized
* jdk1.8 only

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>buffer-trigger</artifactId>
    <version>0.2.8-SNAPSHOT</version>
</dependency>
```

## Usage

```Java

// declare
BufferTrigger<String> buffer = SimpleBufferTrigger.<String, Set<String>> newBuilder() //
        .triggerStrategy(new MultiIntervalTriggerStrategy() //
            .on(10, SECONDS, 1) //
            .on(5, SECONDS, 10) //
            .on(1, SECONDS, 100) //
        ) //
        .consumer(this::out) //
        .setContainer(ConcurrentSkipListSet::new, Set::add) // default is Collections.newSetFromMap(new ConcurrentHashMap<>())
        .build();
        
// enqueue
buffer.enqueue("i'm ok");

// consumer declare
private void out(Collection<String> set) {
	set.forEach(System.out::println);
}

// batch consumer blocking queue
BufferTrigger<String> buffer = BatchConsumeBlockingQueueTrigger.<String> newBuilder() //
                .batchSize(3) //
                .setConsumerEx(this::out) //
                .bufferSize(5) //
                .linger(ofSeconds(2)) //
                .build();
    
```

## Special Thanks

perlmonk with his great team gives me a huge help.
(https://github.com/aloha-app)
