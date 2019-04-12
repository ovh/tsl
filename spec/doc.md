# TSL

TSL stands for Time Series Language and is a proxy to request directly a TSDB. For now you can request a Warp 10 or a Prometheus backend using the following request. It's built to simplify the developer experience to query it's metrics.

## Execute your first TSL query

As example a simple TSL query will simply looks like:

```
select("sys.cpu.nice").where("host=web01").from(1346846400000,to=1346847000005)
```

## TSL main operators

To retrieve data, TSL have a **select** method to choose the metrics name to retrieve, a **where** clauses to select specific labels and a **from** or **last** method to select the temporal period to get.

Once the data are available, with TSL you can apply a lot of functions: from **sampling**, to **grouping**, to **metrics operation** or to **operation between metrics**.

As with TSL the goal is to simplify metrics queries, inside a query a user can store **variables** that will re-used, we will see how to use it. TSL offers the possibilities to fetch data from different backend and to dynamically execute queries on them from a same script using the **connect** method. Besides we will see how to update metrics meta-data (name and labels) and storing the request result in the specified backend 

### Select

The first instructions used to select data is the method **select**.

Select contains only **one** parameter : 
 
* The series name as a string. It can also be asterisk to retrieve all series of the current application.

Example:

```
// Will load the last points of all sys.cpu.nice
select("sys.cpu.nice") 

// Will load the last points of all series of this application (only on a Warp 10 backend)
select(*)
```

> TSL supports native backend. At current time, for **Prometheus** you need to specify the exact classname of the metrics to load. When for **Warp 10**, you can use native regexp. _As example "~sys.*" is a working Warp 10 REGEXP to select all series starting with sys._

### Where 

The **where** method allow the user to filter the metrics to load matching labels clauses.

Where can contains **one** to **n** parameter(s):

* The labels selector string formed as "key=label". 

With the "key=label" string, we use an equals label matcher. The TSL valid matcher are **=**, **~**, **!=** and **!~**. The first one encounters in the string will be used.

Example: 

```
// Will load the last points of sys.cpu.nice series where 'dc' equals 'lga' 
select("sys.cpu.nice").where("dc=lga")

// Will load the last points of sys.cpu.nice series where 'dc' equals 'lga' and have labels 'web'
select("sys.cpu.nice").where("dc=lga", "web~.*")
```

> You can chain as many where clauses as wanted in a TSL query, example: select(...).where(...).where(...) as long as you are defining the data to retrieve.

### Retrieve only series meta-data

TSL includes post-select statements to return only the series names, labels or selectors of the select results. The available methods are:

* The **names** method return the unique names of a series set, example: _.names()_
* The **labels** method return the unique labels maps of a series set. To retrieve a specific label values you can add a label key string as parameter to the **labels** function. It will then return the unique values for this specific label. Examples: _.labels()_, _.labels("host")_
* The **attributes** method return the unique attributes maps of a series set. To retrieve a specific attribute values you can add an attribute key string as parameter to the **attributes** function. It will then return the unique values for this specific attribute. Examples: _.attributes()_, _.attributes("host")_
* The **selectors** method return the unique selectors string of a series set, example: _.selectors()_

### From or Last

The last methods to define the data to retrieve are **last** and **from**. They are used to set the time limits to retrieve the data.

The **from** method is used to select physical time limits. 

From can contains **one** or **two** parameters:

* from: retrieve data **from** this date, can be valid timestamp or date string of the backend
* to: retrieve data **until** this date, can be valid timestamp or date string of the backend

A valid timestamp for **Warp 10** is a long in time unit (on our Metrics platform it's in micro-seconds: 1346846400000 is valid), when a valid timestamp for **Prometheus** may be provided as a Unix timestamp in seconds, with optional decimal places for sub-second precision (on our Metrics platform, you can have timestamp in ms: 1524376786.878 is valid).

A valid date string for **Warp 10** are [ISO 8601 dates string](https://en.wikipedia.org/wiki/ISO_8601) and for **Prometheus** are date in [RFC3339 format](https://www.ietf.org/rfc/rfc3339.txt):  "2018-04-22T00:57:00-05:00" is valid for both backends. 

> By default, if only one parameter is set, it considers that it corresponds to the **from** parameter and will load all data from the current date. Be careful as it can retrieve a lot of data.

When using the from method, you can prefix the parameter with **from** or **to** prefix.

Example: 

```
// Will load all values set after timestamp 0 of sys.cpu.nice series.
select("sys.cpu.nice")
  .from(0)

// Will load all values between two timestamps of sys.cpu.nice series.
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)

// Will load all values between two timestamps of sys.cpu.nice series.
select("sys.cpu.nice")
  .from(from=1346846400000000, to=1346847000006000)

// Will load all values between two timestamps of sys.cpu.nice series.
select("sys.cpu.nice")
  .from(to=1346847000006000, from=1346846400000000)

// Will load all values between two dates of sys.cpu.nice series.
select("sys.cpu.nice")
  .from("2018-04-22T00:57:00-05:00",to="2018-04-22T01:00:00-05:00")

// From a Prometheus backend
select("sys.cpu.nice")
  .from(to=1346847000.005, from=1346846400.000)
```

To resume **from** valid parameters are listed below. A parameter can be optional or mandatory. When a prefix is indicated, it means that this parameter can be set using a prefix name.

| name | type | mandatory | prefix | Complementary information |
|------|------|-----------|--------|---------------------------|
| from | Integer,String | <i class="fas fa-check"></i> | from | Only on Warp 10, [ISO 8601 dates string](https://en.wikipedia.org/wiki/ISO_8601) |
| from | Integer, Double, String |<i class="fas fa-check"></i>| from | Only on Prometheus, [RFC3339 format](https://www.ietf.org/rfc/rfc3339.txt) |
| to | Integer,String | <i class="fas fa-times"></i> | to | Only on Warp 10, [ISO 8601 dates string](https://en.wikipedia.org/wiki/ISO_8601) |
| to | Integer, Double, String |<i class="fas fa-times"></i> | to | Only on Prometheus, [RFC3339 format](https://www.ietf.org/rfc/rfc3339.txt) |

The **last** method is used to select the last recorded datapoints after a valid date.

Last can contains **one** or **three** parameters:

* The first parameter must be a time duration (Prometheus and Warp 10) or a fix number (Warp10). A time duration will fetch all the data points in the time window before the current date or specified timestamp. On a Warp 10 backend, a number to retrieve as many points as specified before the current date or the specified Timestamp. 
* And optionnaly the second or third parameter can be a timestamp or a string date to load data before.
* And optionnaly the second or third parameter can be an other time duration corresponding to a shift duration (loading one hour specified tick).

Example: 

```
// Will load last point before the current date of sys.cpu.nice series.
select("sys.cpu.nice")
  .last(1)

// Will load last minute before the current date of sys.cpu.nice series.
select("sys.cpu.nice")
  .last(1m)

// Will load 10 points before 1528240512000000 of sys.cpu.nice series.
select("sys.cpu.nice")
  .last(10, timestamp=1528240512000000)

// Will load last minute before "2018-04-22T01:00:00-05:00" of sys.cpu.nice series.
select("sys.cpu.nice")
  .last(2m, date="2018-04-22T01:00:00-05:00")

// Will load last minute one hour before NOW of sys.cpu.nice series.
select("sys.cpu.nice")
  .last(2m, shift=1h)
```

To resume **last** valid parameters are listed below. A parameter can be optional or mandatory. When a prefix is indicated, it means that this parameter can be set using a prefix name.

| name | type | mandatory | prefix | Complementary information |
|------|------|-----------|--------|---------------------------|
| span | Duration value | <i class="fas fa-check"></i> | None | first parameter |
| count | Integer | <i class="fas fa-check"></i> | None | Only on Warp 10, first parameter |
| date | String| <i class="fas fa-times"></i> | date | On Prometheus, [RFC3339 format](https://www.ietf.org/rfc/rfc3339.txt) and on Warp 10, [ISO 8601 dates string](https://en.wikipedia.org/wiki/ISO_8601) |
| timestamp | Integer | <i class="fas fa-times"></i> | timestamp | Only on Warp 10 |
| timestamp | Integer, Double | <i class="fas fa-times"></i> | timestamp | Only on Prometheus |
| shift | Duration value |<i class="fas fa-times"></i> | shift | |

### Warp 10 attribute policy

In the case you are using TSL on **Warp 10**, the **attributePolicy** method allow you to choose how to handle the attributes in TSL series set result. You can choose between **merging** the Attributes with the series Labels, between keeping them **splitted** from the labels or simply **remove** them. By default TSL kept the split mode. The **attributePolicy** valid parameter is one of **merge**, **split** or **remove**.

```
select("sys.cpu.nice").where("dc=lga", "web~.*")
  .last(2m, shift=1h)
  .attributePolicy(remove)
```

The **attributePolicy** method should be put right after a select statement, as a **where**, **from** or **last** method but before any further metrics operations.

### Sampling

When collecting servers or application metrics, the data stored are often unsynchronised. To start processing our stored metrics, it's often mandatory to sample the data. Sampling the data corresponds to split metrics data points per time window. All values in this time window are send as parameter to a function that will provide one value as result.

This can be done using the TSL **sampleBy** method.

The **sampleBy** method expects as first parameter (mandatory):

* **span**: that correponds to the time window duration (duration format: 1m, 2M, 10s) 
* or **count**: that correponds to a number of splits of the series as number (1, 2, ...).

The **sampleBy** method expects as second parameter (mandatory):

* An **aggregator** function to use: can be one of **max, mean, min, first, last, sum, join, median, count, percentile, and** or **or**. TSL expects the aggregator to be set as an ident field.

The **sampleBy** method takes also two optionals parameters: 

* A boolean to indicate whether we should keep a relative sampling (true) or use an absolute one (default, and params at false): absolute sampling means that data would be round up (ex: with a 5 minutes span series at time 12:03 it would be 12:05, 12:00, 11:55, when with a relative sampling times would be at 12:03, 11:58, 11:53).
* A sampling policy can be **auto, none, interpolate, next** or **previous**. TSL expects the policy to be set as string (example "auto") or a list of strings, containing the policiy to apply in order. This list is restrained to values equals to **interpolate, next or previous**. Using **interpolate** policy will compute the interpolation of the intermediary values, **next** will fill missing values with the next values it found, and **previous** will fill missing values by the previous value of the series found. The **none** one will let empty missing values. When **auto** means that an interpolation is applied first to field intermediary missing values, previous to fill missing values before the first data-point and next to fill missing values after the last data-point. To fill missing value you can also use the method **fill** as policy. Fill expects a single parameter, the value to fill the series with. When no policy it's set it used **auto** by default.

> The duration format is a number followed by one of **w** for week(s), **d** for day(s), **h** for hour(s), **m** for minute(s), **s** for second(s), **ms** for milli-second(s), **us** for micro-second(s), **ns** for nano-second(s) and **ps** for pico-second(s)

> With a Prometheus back-end, we use the step query parameter to sample the data. It's handled a bit differently as by default Prometheus will sample by the last value recorded (until last 5 minutes). 

>> When using sampleBy in TSL on **Prometheus** you can only set a **span** and an **aggregator** equals to **last** as parameters.

Example:

```
// Will load all values between of sys.cpu.nice series with 1 minute samples (one point per minute), aggegrated using a mean function. 
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, mean)


// Will load all values between of sys.cpu.nice series with 1 minute samples (one point per minute), aggegrated using a max function. 
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(30, max)

// Will load all values between of sys.cpu.nice series with 1 minute samples aggegrated using a mean function. One point per minute when at least one point exists in each minute.
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, mean, "none")

// Will load all values between of sys.cpu.nice series with 1 minute samples aggegrated using a mean function, filling intermediary missing point by a values interpolation and not using a relative sampling.
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, mean, "interpolate", false)

// Valid parameters prefixes
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(span=1m, aggregator=mean, fill="interpolate", relative=false)

// Using a list as fill policy
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(span=1m, aggregator="mean", fill=["interpolate", "next", "previous"], relative=false)

// Using the fill value method policy to fill missing values by Zero
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(span=1m, aggregator="mean", fill=fill(0), relative=false)
  
// Using the fill value method policy to fill missing values by Zero
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(span=1m, aggregator="mean", fill(0), relative=false)
```

To resume **sampleBy** valid parameters are listed below. A parameter can be optional or mandatory. When a prefix is indicated, it means that this parameter can be set using a prefix name.

| name | type | mandatory | prefix | Complementary information |
|------|------|-----------|--------|---------------------------|
| span | Duration value | <i class="fas fa-check"></i> | span | |
| count | Number | <i class="fas fa-check"></i> | count | |
| aggregator | Operator | <i class="fas fa-check"></i> | aggregator | Operator value can be one of: **max, mean, min, first, last, sum, join, median, count, percentile, and, or** |
| fill | String | <i class="fas fa-times"></i> | fill | Fill value can be one of **auto, none, interpolate, next, previous** |
| fill | List of string | <i class="fas fa-times"></i> | fill | Each values of the list can be one of **interpolate, next, previous** |
| relative | Boolean | <i class="fas fa-times"></i> | relative | |

### Group, GroupBy and GroupWithout

When building a metrics data flow, once we sampled the data, we may want to regroup similar metrics. This is what the **group** and **groupBy** methods are build to. The user defines the aggregation function and custom rules to applied to reduce to a single value all metrics values occuring at the same time. 

The **group** method will generate a single series using the specified aggregator.

The group method takes one parameter:

* The aggregator function to use: can be one of **max, mean, min, sum, join, median, count, percentile, and** or **or**. TSL expects the policy to be set as an ident field.

```
// Valid parameters prefix
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "mean")
  .group(sum)
```

The **groupBy** method allow to specify labels to limit the aggegator on series that have the same values for each labels key specified. For example, when using our example, if we specify **dc** and **host**, it will reduce the data into two series: both with "dc=lga" and one with **host** equals to "web01" and the second with "web02". 

The groupBy method takes two to n parameters:

* A labels key as string to group the data on. To select more than one label string you can use a label string list as parameter
* The aggregator function to use: can be one of **max, mean, min, sum, join, median, count, percentile, and** or **or**. TSL expects the policy to be set as an ident field.

Example:

```
// Valid parameters prefix
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "mean")
  .groupBy("dc", mean)

// Valid parameters prefix
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "mean")
  .groupBy(["host","dc"],mean)
```

To resume **groupBy** valid parameters are listed below. A parameter can be optional or mandatory. When a prefix is indicated, it means that this parameter can be set using a prefix name.

| name | type | mandatory | prefix | Complementary information |
|------|------|-----------|--------|---------------------------|
| label | String | <i class="fas fa-check"></i> | None | a label key as first parameter |
| labels | List of string | <i class="fas fa-check"></i> | None | a label key list as first parameter |
| aggregator | Operator | <i class="fas fa-check"></i> | None | Operator value can be one of: **max, mean, min, sum, join, median, count, percentile, and** or **or** as second parameter |

The **groupWithout** methods works the same way as the groupBy one exept it will compute the minimal equivalence classes and then remove the labels given as parameters to group the series on. **groupWithout** behavior is similar to the PromQL aggregation operators.

Example:

```
// Valid parameters prefix
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "mean")
  .groupWithout("dc", mean)

// Valid parameters prefix
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "mean")
  .groupWithout(["host","dc"],mean)
```

When using TSL on prometheus, you can also use the **groupLeft** and **groupRight** to match PromQL **group_left** and **group_right** operators.

### Metrics values operators

Sometimes, we just want to update our series values (adding 2, checking the values with a threshold, rounded the value, compute a rate, and so on). In TSL, we have a large variety of Time series operator available than can be applied directly on a series result. 

This can be done using the TSL **window** method.

The **window** method expects

* At least a **window function** to use: can be one of **max, mean, min, first, last, sum, delta, stddev, stdvar, join, median, count, percentile, and** or **or**. TSL expects the window function to be set as an ident field.
* A single duration time window to compute the **over_time** method on for **Prometheus** or **Warp10**.
* **Warp10** MAP frame supports two parameters as TSL window function a [pre and/or post](http://www.warp10.io/reference/frameworks/framework-map/) parameter. The **pre** and **post** parameters can be a number of points to compute the window on, or a duration if the series was sampled before.

> As Warp 10 is more flexible, you can either specify a duration or a number of points to apply on with the [pre and/or post](http://www.warp10.io/reference/frameworks/framework-map/) parameter.

Example:

```
// With only a duration (Prometheus or Warp10)
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "last")
  .window(mean, 1m)

// With pre and post as durations (Warp10)
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "last")
  .window(sum, 2m, 1m)


// With pre and post as integer (Warp10)
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .window(sum, 2, 10)

// With percentile operator, with pre and post (Warp10)
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .window(percentile, 42, 2, 10)

// With percentile operator, without pre and post (Warp10)
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .window(percentile, 42)

// With join operator, with pre and post (Warp10)
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .window(join, '-', 2, 10)
```

Instead of the window function, the **cumulative** method can aslo be applied. It takes:

* A **window function** to use: can be one of **max, mean, min, first, last, sum, delta, stddev, stdvar, join, median, count, percentile, and** or **or**. TSL expects the window function to be set as an ident field.

This function will apply the function on all windows that appears before each points. This can be useful to complete a cumulative sum on a time series.

```
// Cumulative with sum
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "last")
  .cumulative(sum)


// Cumulative with delta
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "last")
  .cumulative(delta)

// Cumulative with Percentile
select("sys.cpu.nice")
  .from(1346846400000000,1346847000006000)
  .sampleBy(1m, "last")
  .cumulative(percentile, 42)
```

> The **cumulative** operator is not available on **Prometheus**.

#### Arithmetic operators

The following TSL methods can be used to apply arithmetic operators on metrics:

* The **add** operator. Add takes **one number parameter**, example: _.add(2)_
* The **sub** operator. Sub takes **one number parameter**, example: _.sub(2)_
* The **mul** operator. Mul takes **one number parameter**, example: _.mul(2)_
* The **div** operator. Div takes **one number parameter**, example: _.div(2)_
* The **abs** operator. Compute the **absolute value** of all values of the series, example: _.abs()_
* The **ceil** operator. **Round** all values of the series at the nearest integer **above**, example: _.ceil()_
* The **floor** operator. **Round** all values of the series at the nearest integer **below**, example: _.floor()_
* The **round** operator. **Round** all values of the series at the nearest integer, example: _.round()_
* The **ln** operator. Compute values **ln**, example: _.ln()_
* The **log2** operator. Compute values **log2**, example: _.log2()_
* The **log10** operator. Compute values **log10**, example: _.log10()_
* The **logN** operator. Compute values logN of the **number parameter**, example: _.logN(2)_
* The **rate** operator. Compute a **rate** (by default per second when no parameter are sets) or on a specify duration, example: _.rate()_, _.rate(1m)_
* The **sqrt** operator. Compute values **square root**, example: _.sqrt()_
* The **quantize** operator. Compute the amount of **values** inside a **step** on the complete query range or per parameter duration. This generate a single metric per step, based on the label key specified as first parameter. The second parameter corresponds to the step value: it can be a single number or integer value, or a fix step set modelised as a number or integer list. The last optional parameter for the quantize method is the quantize duration. This method can be useful to compute histograms, use example: _.quantize("quantile", [ 0, 10 ], 2m)_, _.quantize("quantile", 0.1)_

> The **logN** operator is not available on **Prometheus**.

#### Remove NaN values in Warp 10

With Warp 10 you can use the [finite mapper](https://www.warp10.io/doc/mapper.finite) to remove the NaN values, you can do the same in TSL:

* The **finite** operator. Remove NaN values, example: _.finite()_

#### Equality operators

The following TSL methods can be used to apply equality operators on metrics:

* The **equal** operator. Only values that are stricly equals to **equal parameter** are kept, example: _.equal(2)_
* The **notEqual** operator. Only values that are not equals to **notEqual parameter** are kept, example: _.notEqual(2)_
* The **greaterThan** operator. Only values that are stricly above to **greaterThan number parameter** are kept, example: _.greaterThan(2)_
* The **greaterOrEqual** operator. Only values that are above or equals to **greaterOrEqual number parameter** are kept, example: _.greaterOrEqual(2)_
* The **lessThan** operator. Only values that are stricly below to **lessThan number parameter** are kept, example: _.lessThan(2)_
* The **lessOrEqual** operator. Only values that are below or equals to **lessOrEqual number parameter** are kept, example: _.lessOrEqual(2)_

#### Limit operators

The following TSL methods can be used to apply limit operators on metrics:

* The **maxWith** operator. MaxWith will test all values to keep only the one **above maxWith parameter** and **replace** all other values per maxWith parameter, example: *.maxWith(2)*
* The **minWith** operator. MinWith will test all values to keep only the one **below minWith parameter** and **replace** all other values per minWith parameter, example: *.minWith(2)*

#### Metrics type convertor

The following TSL methods can be used to convert metrics values:

* The **toboolean** operator used to convert all metrics values to boolean, example: _.toboolean()._
* The **todouble** operator used to convert all metrics values to double, example: _.todouble()._
* The **tolong** operator used to convert all metrics values to long, example: _.tolong()._
* The **tostring** operator used to convert all metrics values to long, example: _.tostring()._

#### Metrics time operators

The following TSL methods can be used to apply time related operators on metrics:

* The **shift** operator used to **shift** all points by a **duration parameter**, example: _.shift(2m)._
* The **day** operator used to replace each points per the **day of the month** of each points (in UTC time), example: _.day()._
* The **weekday** operator used to replace each points per **the day of the week** of each points (in UTC time), example: _.weekday()._
* The **hour** operator used to replace each points per their **hours** (in UTC time), example: _.hour()._
* The **minute** operator used to replace each points per their **minutes** (in UTC time), example: _.minute()._
* The **month** operator used to replace each points per their **month** (in UTC time), example: _.month()._
* The **year** operator used to replace each points per their **year** (in UTC time), example: _.year()._
* The **timestamp** operator used to replace each points per their **timestamp** (in UTC time), example: _.timestamp()._
* The **keepLastValues** operator used to keep the last N values of the operator (from 0 to the current metrics size, by default return only the last metric value), example: _.keepLastValues()._, _.keepLastValues(10)._
* The **keepLastValue** singular form of the previous method, example: _.keepLastValue()._
* The **keepFirstValues** operator used to keep the first N values of the operator (from 0 to the current metrics size, by default return only the first metric value), example: _.keepFirstValues()._, _.keepFirstValues(10)._
* The **keepFirstValue** singular form of the previous method, example: _.keepFirstValue()._
* The **shrink** operator used to shrinks the number of values specified as parameter of each metrics of the set, example: _.shrink(5)._
* The **timeclip** operator used to keep only values in a specific time interval. **timeclip** expects 2 parameters: the last tick of the interval to keep and the duration time to keep before (as number or as duration). You can also specified two ISO8601 string date to set the time interval to keep. With **timeclip**, using the **now** keyword will push NOW as a timestamp in the correct unit time of the platform. Example: _.timeclip(1535641320000000, 2m)_, _.timeclip(now, 200000)_, _.timeclip("2018-04-22T00:57:00-05:00","2018-04-22T01:00:00-05:00")_
* The **timemodulo** operator used to split metrics per a time modulo given as parameter. This will add to each series a new label key (second parameter) and the value of original timestamp quotient. Use example: _.timemodulo(42,"quotient")._
* The **timescale** operator used to multiply each series timestamp by the value set as the method parameter, example: _.timescale(42)._
* The **timesplit** operator used to split timeseries based on **quiesce** periods. The first parameter is the time duration value of the quiesce period, it can be a duration value, a long or the keyword now. The second one in the minimal amount of points to keep a new series (to reduce noise when creating series), and the last one the new label key for each split series. example: _.timesplit(now, 42, 'test')._, _.timesplit(1h, 4, 'test')._, _.timesplit(20000000, 1, 'test')._

For **keepLastValue(s)** and **keepFirstValue(s)** functions, if the parameter specified is greater than the actual size of the metric, those functions will then return the complete metrics. 

The **keepFirstValue(s)**, **shrink**, **timeclip**, **timemodulo**, **timescale** and **timesplit** are currently **not** supported on a Prometheus backend.

The**keepLastValue(s)** works on a Prometheus backend, calling an instant query. This means that on a Prometheus **keepLastValue(s)** expects no parameters or only the value `1`.


 
### Metrics sort 

TSL introduces some methods to sort metrics by their samples values.

* The **sort** operator used to sort metrics data by their globals **mean** value in **ascending** order. Use example: _.sort()._
* The **sortDesc** operator used to sort metrics data by their globals **mean** value in **descending** order. Use example: _.sortDesc()._
* The **sortBy** operator used to sort metrics data according to the result of a **global operator** in **ascending** order. The operator function can be one of: **last, first, max, mean, min, sum, median, count, percentile, and** or **or**. Use example: _.sortBy(max)._, _.sortBy(percentile, 42)._
* The **sortDescBy** operator used to sort metrics data according to the result of a **global operator** in **descending** order. The operator function can be one of: **last, first, max, mean, min, sum, median, count, percentile, and** or **or**. Use example: _.sortDescBy(max).
* The **topN** operator used to get the top N series (sorted by their globals **mean** value in **descending** order) Use example: _.topN(2)._
* The **bottomN** operator used to get the lowest N series (sorted by their globals **mean** value in **ascending** order). Use example: _.bottomN(2)._
* The **topNBy** operator used to get the top N series (sorted according to the result of a **global operator** in **descending** order. The operator function can be one of: **last, first, max, mean, min, sum, median, count, percentile, and** or **or**). Use example: _.topNBy(2, min)._, _.topNBy(2, percentile, 42)._
* The **bottomNBy** operator used to get the lowest N series (sorted according to the result of a **global operator** in **ascending** order. The operator function can be one of: **last, first, max, mean, min, sum, median, count, percentile, and** or **or**). Use example: _.topNBy(2, max)._

> The **sortBy**, **sortDescBy**, **topNBy** and **bottomNBy** operators are not available for **Prometheus**.

### Metrics filtering

TSL includes a few methods to filter the metrics result set: 

* The **filterByLabels** method to keep only the metrics matching some labels rules defined as parameters. **filterByLabels** expects at least one label clause string, and optionally as many as needed. Use example: _.filterByLabels("label~42.*", "host=server-01")._
* The **filterByName** method to keep only the metrics matching a name rule defined as parameters. **filterByName** expects a single string rule. Use example: _.filterByName("test")_, _.filterByName("~test")_
* The **filterByLastValue** method to keep only the metrics matching a rule on their last value defined as parameters. **filterByLastValue** expects at least one string rule, and optionally as many as needed. Use example: _.filterByLastValue(">=42")_, _.filterByName("!='mystring'")_. The valid **filterByLastValue** parameters are **<=**, **<**, **!=**, **=**, **>=** and **>**.

### Metrics operators on metrics sets

When we load several set of data, we may want to apply operation on metrics sets. TSL allow us to apply operators on metrics. 

#### Metrics operators

A metrics operators will apply an operation on several set of metrics. 

_For example: we can add the values of a first series with a second one. Value will be added when ticks have an exact match, this is why it's important to sample the data before executing such an operation._

* The **add** operator between metrics sets, example: _add(select(...), select(...), ...)_
* The **sub** operator between two metrics sets, example: _sub(select(...), select(...))_
* The **mul** operator between metrics sets, example: _mul(select(...), select(...), ...)_
* The **div** operator between two metrics sets, example: _div(select(...), select(...))_
* The **and** operator between metrics sets, example: _and(select(...), select(...), ...)_
* The **or** operator between metrics sets, example: _or(select(...), select(...), ...)_
* The **equal** operator between metrics sets, example: _equal(select(...), select(...), ...)_
* The **notEqual** operator between metrics sets, example: _notEqual(select(...), select(...), ...)_
* The **greaterThan** operator between metrics sets, example: _greaterThan(select(...), select(...), ...)_
* The **greaterOrEqual** operator between metrics sets, example: _greaterOrEqual(select(...), select(...), ...)_
* The **lessThan** operator between metrics sets, example: _lessThan(select(...), select(...), ...)_
* The **lessOrEqual** operator between metrics sets, example: _lessOrEqual(select(...), select(...), ...)_
* The **mask** operator to use a [Warp 10 mask](https://www.warp10.io/doc/op.mask), example: _mask(select(...).toboolean(), select(...))_
* The **negmask** operator to use a [Warp 10 negmask](https://www.warp10.io/doc/op.negmask), example: _negmask(select(...).toboolean(), select(...))_


Example:

```
// Valid parameters prefix
add(
  select("sys.cpu.nice")
    .from(1346846400000000,1346847000006000)
    .where("host=web01")
    .sampleBy(1m, "mean"),
  select("sys.cpu.nice")
    .from(1346846400000000,1346847000006000)
    .where("host=web02")
    .sampleBy(1m, "mean")
)
```

> By default, on **Warp 10** only one metrics will be computed as result except if we use the **on** and/or **ignoring**  method explained below.

> By default, on **Prometheus** the minimal equivalence class matching a maximum of labels will be computed as result except if we use the **on** and/or **ignoring**  method explained below.

#### On method

To limit operation on specific labels, the method **on** can be-used post a metrics operator one.

For example the following TSL query will return two series one where all values of the _"web01"_ host series are summed and a second one for the _"web02"_ host series.

```
// Add on label "host"
add(
  select("sys.cpu.nice")
    .from(1346846400000000,1346847000006000)
    .sampleBy(1m, "mean"),
  select("sys.cpu.nice")
    .from(1346846400000000,1346847000006000)
    .sampleBy(1m, "mean")
).on("host")
```

#### Ignoring method

The **Ignoring** method will remove the selected labels of the equivalence class for the operator method.

Example:

```
// Compute an add on all series
add(
  select("sys.cpu.nice")
    .from(1346846400000000,1346847000006000)
    .where("host=web01")
    .sampleBy(1m, "mean"),
  select("sys.cpu.nice")
    .from(1346846400000000,1346847000006000)
    .where("host=web02")
    .sampleBy(1m, "mean")
).ignoring("host")
```

### Variables

TSL allow the user to set it's own variable. Just set a name followed by an "=" sign. 

To reuse a variable, just use it's name. To execute a query store in a variable, just write the variable name in a new line.

Example:

```
// Store a two minutes duration
customDuration = 2m

// Store a series name
seriesName = "sys.cpu.nice"

// Store a label name
labelName = "host=web02"

// Store a number
myNumber = 100

// Store a select instruction and it's operation
mySelect = select(seriesName)
              .from(1346846400000000,1346847000006000)
			        .where(labelName)
			        .sample(30s)
			        .add(100)

// Get it's result
mySelect
```

```
// Store a label name
labelName = "host=web02"

// Store a single select
mySelect = select("sys.cpu.nice").from(1346846400000000,1346847000006000)

// Apply post select operation and get result 
mySelect.where(labelName).ln()
mySelect.where(labelName).add(100)
```

```
// Store a label name
labelName = "host=web02"

// Use variables in operation
mySelect = select("sys.cpu.nice").from(1346846400000000,1346847000006000)

add(mySelect.where(labelName).ln(),mySelect
   .where("host=web01"))

// Store multiples series operation result in variable
addSave = add(mySelect.where(labelName).ln(),mySelect
   .where("host=web01"))

// Get multiples series operation result from addSave variable
addSave.on("host").add(100)
```

#### Use String templates with variables

You can define a variable and re-use it directly inside a TSL string using a template as shown in the example below:

```
host = "my.host"

select("my.series")
   .where(["host=${host}"])
```

#### TSL Lists

You can declare and use TSL lists in a variable:

```
labelsNames = ["host=web02", "dc~.*"]
```

On a TSL list you can apply:

* The **add** method to add elements to the current list. Use example : _.add("test=42")_
* The **remove** method to remove elements of the current list. Use example : _.remove("test=42")_

For example to use it in a where statement:
```
select("sys.cpu.nice").where(labelsNames)
```

### Connect

In TSL, we can directly use the Connect method to update the set the backend on which queries are processed. For a warp10 backend it's:

```
connect("warp10","http://localhost:9090", "TOKEN")
```

For a prometheus it's
```
connect("prometheus","http://localhost:9090")
```

or with a user/password if Prometheus is behind a basic auth:

```
connect("prometheus","http://localhost:9090","user","pwd")
```

#### Series meta operator

The update metrics meta-data in TSL you can use one of the following function:

* The **addPrefix** to add a **prefix** to each metrics name of a set. Use example: _.addPrefix("prefix")._
* The **addSuffix** to add a **suffix** to each metrics name of a set. Use example: _.addSuffix("suffix")._
* The **rename** to rename each metrics of a set. Use example: _.rename("newName")._
* The **renameBy** to rename each metrics per one of it's labels. Use example: _.renameBy("host")._
* The **removeLabels** to remove one or several labels of a metrics set. Use example: _.removeLabels("host", "dc")._
* The **renameLabelKey** to rename a label key. Use example: _.renameLabelKey("dc", "Data-center")._
* The **renameLabelValue** to rename a label value. Use example: _.renameLabelValue("dc", "new")._
* The **renameLabelValue** to rename a label value matching a regexp. Use example: _.renameLabelValue("dc", "lg.*", "new-dc")._
* The **renameTemplate** expect a Template string to rename a series. The `${this.name}` corresponds to the current series name, the `${this.labels.key}` to a series label key. Use example: _.renameTemplate("my.new.series.${this.name}.is.great"), .renameTemplate('${this.labels.hostname} (${this.labels.datacenter}) ${this.labels.hostname} ${this.name} ${ this.name }')._

> None of those methods are currently available for **Prometheus**. 


#### Create a series 

At the same level as the select statement, you can use the **create** one. It will create a Time Series List. Create then accepts n series functions as parameter.

Example: 

```tsl
create(series(), series())
```

The **series** method is used to create a Time Series, only in Warp 10 for now, you can give the Time Series name as parameter. To set the created series labels, you can use the **setLabels** method. You can also use the **setValues** to add values to this newly created series.

The **setValues** takes n parameter, the first one (optional) is the base Timestamp of the values (by default zero). Then the other are a two elements array composed of a timestamp (that would be add to the base one) and the value to set. Use example: _.setValues([0, 1], [100, 2])._

The **setLabels** takes a single parameter: a labels string list where the key and values are split per the equals symbol.

At the end of the create statement, all other Time Series set methods can be apply on.

A more complex but valid tsl statement to create 2 Time Series would be:

```tsl
create(series("test").setLabels(["l0=42","l1=42"]).setValues("now", [-5m, 2], [0, 1]).setValues("now",[2m, 3]),series("test2").setLabels(["l0=42","l1=42"]).setValues(now, [-5m, 2], [0, 1]))
	 .sampleBy(30s, max)
``` 



#### Global series operator

TSL can also be used to store query result back on the backend. 
This can be done using the **store** method. Store expects a token as unique parameter. Use example:  _.store("WRITE\_TOKEN")._

> **store** is only avaible on a Warp 10 backend.

To resets counters values the method **resets** can be applied in TSL. Use example:  _.resets("host")._

## Going further

You can exchange with us here or on our [gitter room](https://gitter.im/ovh/metrics-TSL).

Any feedback or idea about TSL would be warmly welcome!