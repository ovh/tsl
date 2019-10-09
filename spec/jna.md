
## How to use the TSL library in JAVA

The TSL .so library was build based on [this github tutorial](https://github.com/vladimirvivien/go-cshared-examples).

### So library

First of all, you need to build TSL so library using:

```sh
make so
```

### The TSL exposed function

TSL exposes a single method (defined in [libso.go](../so/libso.go)): 

```tsl
TslToWarpScript(tslQuery string, token string, allowAuthenticate bool, lineStart int, defaultTimeRange string, defaultSamplers string) *C.char 
```

The TslToWarpScript method expects 6 parameters: 
 - tslQuery: The TSL string to parse,
 - token: The TSL query Token,
 - allowAuthenticate: Will TSL natively raise the user limit by authenticating the WarpScript stack,
 - lineStart: Where the user TSL query start (remove auto-generated prefix. It's usefull when using a tool as Grafana which prefix a script by setting its own variable),
 - defaultTimeRange: Default time range to FETCH data (can be usefull for a tool as Grafana, Set to "" to use no defaultTimeRange),
 - defaultSamplers: Default query sampler (Set to "" to use no sampler).

TslToWarpScript will then returned the string corresponding to the generated WarpScript query produced by the user TSL query. 

### Example use of TSL in JAVA

You can create a working TSL client class in JAVA as:

```java
package io.metrics.tsl;

import com.sun.jna.*;
import java.util.*;

public class Client {
    public interface TSL extends Library {
        // GoSlice class maps to:
        // C type struct { void *data; GoInt len; GoInt cap; }
        public class GoSlice extends Structure {
            public static class ByValue extends GoSlice implements Structure.ByValue {}
            public Pointer data;
            public long len;
            public long cap;
            protected List getFieldOrder(){
                return Arrays.asList(new String[]{"data","len","cap"});
            }
        }

        // GoString class maps to:
        // C type struct { const char *p; GoInt n; }
        public class GoString extends Structure {
            public static class ByValue extends GoString implements Structure.ByValue {}
            public String p;
            public long n;
            protected List getFieldOrder(){
                return Arrays.asList(new String[]{"p","n"});
            }
        }

        // Foreign functions
        public GoString.ByValue TslToWarpScript(GoString.ByValue tslQuery, GoString.ByValue token, Boolean allowAuthenticate, long lineStart, GoString.ByValue defaultTimeRange, GoString.ByValue defaultSamplers);
    }

    static public void main(String argv[]) {
        TSL tsl = (TSL) Native.loadLibrary(
                "/PATH/TO/tsl.so", TSL.class);

        TSL.GoString.ByValue tslQuery = new TSL.GoString.ByValue();
        tslQuery.p = "select(\"os.cpu\")";
        tslQuery.n = tslQuery.p.length();

        TSL.GoString.ByValue token = new TSL.GoString.ByValue();
        token.p = "From java";
        token.n = token.p.length();

        TSL.GoString.ByValue defaultTimeRange = new TSL.GoString.ByValue();
        defaultTimeRange.p = "";
        defaultTimeRange.n = defaultTimeRange.p.length();

        TSL.GoString.ByValue defaultSamplers = new TSL.GoString.ByValue();
        defaultSamplers.p = "";
        defaultSamplers.n = defaultSamplers.p.length();

       // final PointerByReference ptrRef = new PointerByReference();
        TSL.GoString val = tsl.TslToWarpScript(tslQuery, token, true, 0, defaultTimeRange, defaultSamplers);

        System.out.println("WarpScript: " + val.p);
    }
}
```
