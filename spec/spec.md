```go
// read foo metric for the last 1h
select('foo')                        // 
select('foo').where('key=value')
select('foo').where('key~val.*')
select('foo').where('host=srv1','dc~(gra|rbx)')

// Explicit Auth context 
connect('API', 'TOKEN').select('foo')


// Get by Date
select('foo').from(@ISO8601, to=@ISO8601)    // Date are ISO8601, from is oldest and to is newest. to() is optional
select('foo').from('2007-04-22T01:00:00-05:00',to='2017-04-22T01:00:00-05:00')  // Date are ISO8601

// Get by time range
select('foo').last(@timespan, @args...)
select('foo').last(1h, shift=1h)                     // now is a special keyword for the current time at execution
select('foo').last(1h, timestamp=1528240512000000)   //  Get last element from now, or timestamp if provided - Timestamp is in PTU (platform time unit)
select('foo').last(10, date=...)                     // Get last element from now, or date if provided
select('foo').last(1h)                               // without timestamp, now is assumed
select('foo').last(1h).to('2017-04-22T01:00:00-05:00')   // without timestamp, now is assumed

// Get by last n elements
select('foo').last(@number_of_elements, @args...)
select('foo').last(10)                // Get 10 last element from now

// !! 'from' and 'last' are mutualy exclusive

// Filter by labels
select('foo').where(@key=@value)
select('foo').where('dc=gra')
select('foo').where('dc=~(gra|rbx)','rack=001')


// Downsample / bucketize
select('foo').sampleBy(@bucket_span, @aggregator, @args...)
select('foo').sampleBy(5m, max)
select('foo').sampleBy(5m, mean)                     // 5m span from 12:03 : 12:05, 12:00, 11:55   / Absolute is default
select('foo').sampleBy(5m, mean, relative=true)      // 5m span from 12:03 : 12:03, 11:58, 11:53  

select('foo').sample(mean)

// Auto Fill
select('foo').sampleBy(1h, mean)        // Default fill policy : interpolate + fillprefious + fillnext
select('foo').sampleBy(1h, mean, fill=next)
select('foo').sampleBy(1h, mean, fill=previous)
select('foo').sampleBy(1h, mean, fill=auto)
select('foo').sampleBy(1h, mean, fill=interpolate)
select('foo').sampleBy(1h, mean, fill=none)

// Aggregate / reduce
select('foo').groupBy(@tag_key, @aggregator)   // groupBy() applies a sampleBy('1m', last) if no sampling has already been done
select('foo').groupBy('host', sum)

select('foo').group(sum)                         // Idem without By 

// Transform values
select('foo').mul(8)
select('foo').div(1000)
select('foo').add(1)
select('foo').rate()
select('foo').rename('bar')          //  rename the series 
select('foo').renameBy(@tag_key)    // rename the series with the tag value
select('foo').cumulativeSum()       // 
select('foo').last(1d, shift=1d).shift(1d)  // Shifting a series

// Filter values
select('foo').equal(0)
select('foo').notEqual(0)
select('foo').greaterThan(100)   // filter values above 100
select('foo').greaterOrEqual(0)
select('foo').lessThan(10)
select('foo').lessOrEqual(0)
select('foo').max(10)
select('foo').min(0)
select('foo').mean(pre=1, post=1)

// example : 
select('foo').greaterThan(100).lessThan(10).equal(0)



// Simple interface with rate and bytes to bits convertion
select('os.net.bytes').where('host=h1').last(1d).sample(last).rate().mul(8)




// TSQL Extention : data flow


// Data Flow


foo = select('foo').sample(max).groupBy('host', sum)
bar = select('bar').sample(max).groupBy('host', sum)

foobar = add(foo, bar, args...)
foobar = minus(foo, bar, args...)
foobar = mul(foo, bar, args...)
foobar = div(foo, bar, args...)



// Data flow example with shifted comparison
a = select('os.bytes').last(1d).sample(max).groupBy(['host','iface'], sum).mul(8)
b = select('os.bytes').last(1d, shift=1d).sample(max).groupBy(['host','iface'], sum).mul(8).shift(1d)
c = minus(a, b, args...)
(a, b, c)

a2 = mul(a,a)

// Apply the same processing on a tuple of series 
a = select('os.bytes').last(1w)
b = select('os.bytes').last(1w, shift=1w).shift(1w)
(a, b) = (a, b).sample(max).groupBy(['host','iface'], sum).mul(8)
c = a - b
(a, b, c)





// TSQL Extention : control structure


// TSQL Extention : UDF


// greaterThan(), lessThan(), sum(), max() are short mapper
// .map() can still be evoked for in depth mapper usage : 

//get('foo').map(lt(10), @pre, @post, @occurencies)
//get('foo').map(lt(10), 0, 0, 0)   // same as .lt(10)
//get('foo').map(rate, 1, 0, 0)   // same as .rate()
select('foo').map(myFunc, pre=1, post=0, occurencies=0)   // same as .rate()

myFunc = func (args... ) {
}
```