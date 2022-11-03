use polars::{prelude::*};
pub mod event_log;
pub mod dfg;
use dfg::{Dfg};





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
    
    let df: DataFrame = CsvReader::from_path("E:/Rust/20321.csv")
    //let mut df: DataFrame = CsvReader::from_path("e:/rust/rust-pm4py/example.csv")
                .unwrap()
                .with_delimiter(b',')
                .with_dtypes_slice(Option::Some(&dtypes))
                .finish()
                .unwrap();
    

    let mut log = event_log::EventLog::new();
    
    log.create_event_log(&df);

    let mut dfg: Dfg = Dfg::new();
    dfg.create_dfg_from_eventlog(&mut log, &df);
    dfg.visualize_dfg();
    
    //for v in ret.values_mut() {
    //    let mut temp_vec = Vec::<i64>::with_capacity(1);
    //    temp_vec.push(median(v));
    //    *v = temp_vec;
    //}
    
    //println!("{:?}", ret);

    //println!("{}", tuples[0][0].0.act1);
    //println!("{}", tuples[0][0].0.act2);
    //println!("{}", tuples[0][0].1);
}





/* 
fn test_multithreaded_tuples_for_dfg() {
    let mut tuples: Vec<Vec<(DfgActivities, i64)>> = Vec::<Vec::<(DfgActivities, i64)>>::new();
    for trace in log.traces.iter() {
        let mut curr_tuples: Vec<(DfgActivities, i64)> = Vec::<(DfgActivities, i64)>::new();
        for i in 1..trace.events.len() {
            let activities: DfgActivities = DfgActivities { 
                act1: trace.events[i-1].get("concept:name").unwrap().to_string(),
                act2: trace.events[i].get("concept:name").unwrap().to_string()
            };
            curr_tuples.push((
                (activities),
                (
                    NaiveDateTime::from(trace.events[i].get("time:timestamp").unwrap())  - NaiveDateTime::from(trace.events[i-1].get("time:timestamp").unwrap())
                ).num_seconds()
            ));
        }
        tuples.push(curr_tuples);
    }
}
*/

/* 
fn test_multithreaded_ret_creation_dfg() {
    let mut ret: AHashMap<DfgActivities, Vec<i64>> = AHashMap::<DfgActivities, Vec<i64>>::new();
    for el in tuples {
        for couple in el {
            match ret.get_mut(&couple.0) {
                Some(v) => v.push(couple.1),
                None => {
                    let mut v = Vec::<i64>::with_capacity(1);
                    v.push(couple.1);
                    ret.insert(couple.0, v);
                },
            }
        }
    }
}
*/

/*
fn test_multithreaded_creation_dfg() {
    for v in ret.values_mut() {
        let mut temp_vec = Vec::<i64>::with_capacity(1);
        temp_vec.push(median(v));
        *v = temp_vec;
    }
}
*/


