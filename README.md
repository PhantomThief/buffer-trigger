buffer-trigger
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
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```Java

CollectionBufferTrigger<String, Collection<String>> buffer = CollectionBufferTrigger
	.<String, Collection<String>> newBuilder() //
	.on(5, TimeUnit.SECONDS, 10, i -> out("trig:1:" + i)) //
	.on(10, TimeUnit.SECONDS, 15, i -> out("trig:2:" + i)) //
	.fixedRate(6, TimeUnit.SECONDS, i -> out("trig:3:" + i)) //
	.build();
Random rnd = new Random();
for (int i = 0; i <= 100; i++) {
	String e = i + "";
	System.out.println("enqueue:" + i);
	buffer.enqueue(e);
	Thread.sleep(rnd.nextInt(1000));
}
    
```