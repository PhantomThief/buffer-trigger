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
    <version>0.1.0</version>
</dependency>
```

## Usage

```Java

BaseBufferTrigger<String> buffer = BaseBufferTrigger.<String, List<String>> newBuilder() //
        .on(5, TimeUnit.SECONDS, 10, i -> out("trig:1:" + i)) //
        .on(10, TimeUnit.SECONDS, 15, i -> out("trig:2:" + i)) //
        .fixedRate(6, TimeUnit.SECONDS, i -> out("trig:3:" + i)) //
        .setContainer(() -> Collections.synchronizedList(new ArrayList<String>()),
                List::add) // default container is Collections.synchronizedSet(new HashSet<>())
        .build();
Random rnd = new Random();
for (int i = 0; i <= 100; i++) {
    String e = i + "";
    System.out.println("enqueue:" + i);
    buffer.enqueue(e);
    Thread.sleep(rnd.nextInt(1000));
}
    
```

## Know issues

on(...) trigger's count didn't ensure the callback function's count is smaller than you gave, for that callback is async run in another thread and when it started the buffer may continue accept data. If you wanna ensure the callback count, a better method is using a bounded blocking queue as container and block the enqueue operation.
