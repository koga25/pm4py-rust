use polars::prelude::*;
use ahash::AHashMap;
use row::Row;

struct Traces<'a> {
    attributes: AHashMap<&'a str, &'a AnyValue<'a>>,
    events: Vec<&'a AHashMap<&'a str, AnyValue<'a>>>
}

struct EventLog<'a> {
    traces: Vec<Traces<'a>>,
    mapping: AHashMap<&'a AnyValue<'a>, usize>,
}

fn main() {
    

    let dtypes: [DataType; 12] = [
        DataType::Utf8,
        DataType::Utf8,
        DataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Utf8,
        DataType::Utf8,
        DataType::Int64,
        DataType::Int64,
        DataType::Float64,
        DataType::Utf8,
        DataType::Utf8,
        DataType::Utf8,
    ];
    
    let df: DataFrame = CsvReader::from_path("e:/rust/testing/src/20321.csv")
                .unwrap()
                .with_delimiter(b',')
                .with_dtypes_slice(Option::Some(&dtypes))
                .finish()
                .unwrap();


    use std::time::Instant;
    let now = Instant::now();

    let unique_cases = df["case:concept:name"].n_unique().unwrap();
    
    let (df_height, row_len): (usize, usize) = df.shape();
    let mut stream: Vec<AHashMap<&str, AnyValue>> = Vec::<AHashMap<&str, AnyValue>>::with_capacity(df_height);
    let column_names: Vec<&str> = df.get_column_names();
    let mut row: Row = df.get_row(0);
    println!("{} columns", row_len);
    println!("{} rows", df_height);


    for index in 0..df_height {
        df.get_row_amortized(index, &mut row);
        let mut trace: AHashMap<&str, AnyValue> = AHashMap::<&str, AnyValue>::new();
        for column in 0..row_len {
            unsafe {
                trace.insert(column_names[column], row.0.get_unchecked(column).clone());
            }
        }
        stream.push(trace);
    }



    
    let mut event_log: EventLog = EventLog {
        traces: Vec::<Traces>::with_capacity(unique_cases),
        mapping: AHashMap::<& AnyValue, usize>::new()
    };
    let case_glue:&str = "case:concept:name";
    let case_attribute_prefix:&str = "case:";
    let DEFAULT_TRACEID_KEY:&str = "concept:name"; 
    let mut index = 0 as usize;
    for dictionary in stream.iter() {
        let mut found: bool = false;
        //temp_dict.remove(case_glue);
        let glue = dictionary.get(case_glue).unwrap();
        match event_log.mapping.get(glue) {
            Some(v) => {
                event_log.traces[*v].events.push(dictionary);
                //dictionary.remove(case_glue);
            },
            None => {
                let mut events = Vec::<&AHashMap<&str, AnyValue>>::with_capacity(1);
                events.push(dictionary);
                let mut attributes: AHashMap<&str, &AnyValue> = AHashMap::<&str, &AnyValue>::new();
                attributes.insert(DEFAULT_TRACEID_KEY, glue);
                let temp_event_log: Traces = Traces { 
                    attributes: attributes, 
                    events: events 
                };
                event_log.traces.push(temp_event_log);
                event_log.mapping.insert(glue, event_log.traces.len() - 1);
            }
        }
    }
    println!("zzzz\n");
    for (k, v) in event_log.traces[0].attributes.iter() {
        println!("{}, {}", k, v);
    }
    println!("######################################################################");
    for (k, v) in event_log.traces[0].events[0].iter() {
        println!("{}, {}", k, v);
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}

