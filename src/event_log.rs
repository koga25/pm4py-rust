
use polars::prelude::*;
use ahash::AHashMap;
use rayon::prelude::*;

#[derive(PartialEq, Debug)]
pub struct Traces<'a> {
    pub attributes: AHashMap<&'a str, AnyValue<'a>>,
    pub events: Vec<AHashMap<&'a str, AnyValue<'a>>>
}

#[derive(PartialEq, Debug)]
pub struct EventLog<'a> {
    pub traces: Vec<Traces<'a>>,
    //stream is a temporary helper variable to make it faster to construct the eventlog.
}

impl<'a> EventLog<'a> {
    pub(crate) fn new() -> Self {

        EventLog { 
            traces: Vec::<Traces>::new(),
        }
    }

    pub fn create_event_log(&mut self, df: &'a DataFrame) {

        let case_glue: &str = "case:concept:name";
        let default_traceid_key:&str = "concept:name"; 
        let group = &mut df.groupby([case_glue])
            .unwrap();
        let groups: &mut GroupsProxy;
        unsafe {
            groups = group.get_groups_mut();
        }
        groups.sort();
        
        self.traces.reserve(groups.len());
                
        self.traces.par_extend((0..groups.len())
            .into_par_iter()
            .map(|x| {
                let attr = AHashMap::<&str, AnyValue>::new();
                let ev = Vec::<AHashMap<&str, AnyValue>>::with_capacity(groups.get(x).len());
                Traces { 
                    attributes: attr, 
                    events: ev 
                }
            }));

        let column_names: Vec<&str> = df.get_column_names();

        let mut case_concept_name_column: usize = 0;
        for (idx, name) in column_names.iter().enumerate() {
            if *name == case_glue {
                case_concept_name_column = idx;
            }
        }

        let temp_row_indexes =  groups.unwrap_idx().all();
        let mut row_indexes = temp_row_indexes.to_vec();
        row_indexes.par_sort();
        self.traces.par_iter_mut()
            .enumerate()
            .for_each(|(idx, trace)| {
                let mut row = df.get_row(row_indexes[idx][0] as usize);
                unsafe {
                    trace.attributes.insert(default_traceid_key, row.0.get_unchecked(case_concept_name_column).to_owned());
                }
                for (events_idx, row_idx) in row_indexes[idx].iter().enumerate() {
                    trace.events.push(AHashMap::<&str, AnyValue>::new());
                    df.get_row_amortized(*row_idx as usize, &mut row);
                    for column in 0..column_names.len() {
                        if column == case_concept_name_column { continue }
                        unsafe {
                            trace.events[events_idx]
                                .insert(column_names[column], row.0.get_unchecked(column).to_owned());
                        }
                    }
                }
                
            });
        //println!("{:?}", self.traces[0]);

    }
}



//#[cfg(test)]
//fn test_multithread_stream_creation() {
//
//    let mut stream: Vec<AHashMap<&str, AnyValue>> = Vec::<AHashMap<&str, AnyValue>>::new();
//    let mut stream2: Vec<AHashMap<&str, AnyValue>> = Vec::<AHashMap<&str, AnyValue>>::new();
    //self.stream.reserve(df.height());
    //let unique_cases = df["case:concept:name"].n_unique().unwrap();
    //self.traces.reserve(unique_cases);
    //let (df_height, row_len): (usize, usize) = df.shape();
//
    //let column_names: Vec<&str> = df.get_column_names();
    //let mut row: Row = df.get_row(0);
    //let now = Instant::now();
//
    //for index in 0..df_height {
    //    df.get_row_amortized(index, &mut row);
    //    let mut trace: AHashMap<&str, AnyValue> = AHashMap::<&str, AnyValue>::new();
    //    for column in 0..row_len {
    //        unsafe {
    //            trace.insert(column_names[column], row.0.get_unchecked(column).clone());
    //        }
    //    }
    //    self.stream.push(trace);
    //}
    //stream2.par_extend((0..df_height).into_par_iter().map(|x| AHashMap::<&str, AnyValue>::new()));
    //df.get_columns()
    //    .par_iter()
    //    .
    //stream2.par_iter_mut()
    //    .enumerate()
    //    .for_each(|(x, d)| {
    //        let row2 = df.get_row(x);
    //        for column in 0..row_len {
    //            unsafe {
    //                d.insert(column_names[column], row2.0.get_unchecked(column).clone());
    //            }
    //        }
    //    });
    //for index in 0..df_height {
    //    df.get_row_amortized(index, &mut row);
    //    let mut trace: AHashMap<&str, AnyValue> = AHashMap::<&str, AnyValue>::new();
    //    for column in 0..row_len {
    //        unsafe {
    //            trace.insert(column_names[column], row.0.get_unchecked(column).clone());
    //        }
    //    }
    //    stream2.push(trace);
    //}
//    assert_eq!(stream, stream2, "not equal");     
//}
/* 
fn test_eventlog_values() {
    self.stream.reserve(df.height());
        let (df_height, row_len): (usize, usize) = df.shape();
    
        let column_names: Vec<&str> = df.get_column_names();
        let now = Instant::now();

        self.stream.par_extend((0..df_height)
            .into_par_iter()
            .map(|x| AHashMap::<&str, AnyValue>::new()));

        self.stream.par_iter_mut()
            .enumerate()
            .for_each(|(x, d)| {
                let row = df.get_row(x);
                for column in 0..row_len {
                    unsafe {
                        d.insert(column_names[column], row.0.get_unchecked(column).clone());
                    }
                }
            });

    for dictionary in self.stream.drain(0..self.stream.len()) {
            let glue = dictionary.get(case_glue).unwrap().clone();
            match mapping.get(&glue) {
                Some(v) => {
                    let events_len = self.traces[*v].events.len();
                    self.traces[*v].events.push(dictionary);
                    self.traces[*v].events[events_len].remove(case_glue);
                },
                None => {
                    let mut events = Vec::<AHashMap<&str, AnyValue>>::with_capacity(1);
                    events.push(dictionary);
                    let mut attributes: AHashMap<&str, AnyValue> = AHashMap::<&str, AnyValue>::new();
                    attributes.insert(default_traceid_key, glue.clone());
                    let temp_event_log: Traces = Traces { 
                        attributes: attributes, 
                        events: events 
                    };
                    let events_len = self.traces.len();
                    self.traces.push(temp_event_log);
                    mapping.insert(glue, events_len);
                    self.traces[events_len].events[0].remove(case_glue);
                }
            }
        }
}
*/
