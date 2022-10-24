use graphviz_rust::attributes::fontsize;
use graphviz_rust::attributes::{NodeAttributes, color_name, shape, style, GraphAttributes};
use graphviz_rust::cmd::{CommandArg, Format};
use polars::{prelude::*, export::chrono::NaiveDateTime};
use ahash::{AHashMap, AHashSet};
use std::collections::hash_map::DefaultHasher;
use std::process::Command;
use std::{time::Instant};
pub mod event_log;
use event_log::{DfgActivities};
use rayon::prelude::*;
use graphviz_rust::{dot_generator::*, exec};
use graphviz_rust::dot_structures::*;
use graphviz_rust::printer::{PrinterContext, DotPrinter};
use std::hash::{Hash, Hasher};

use crate::event_log::Dfg;




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
    
    let mut df: DataFrame = CsvReader::from_path("e:/rust/testing/src/20321.csv")
    //let mut df: DataFrame = CsvReader::from_path("e:/rust/rust-pm4py/example.csv")
                .unwrap()
                .with_delimiter(b',')
                .with_dtypes_slice(Option::Some(&dtypes))
                .finish()
                .unwrap();
    

    for _i in 0..20 {
        let df1: DataFrame = CsvReader::from_path("e:/rust/testing/src/20321.csv")
            .unwrap()
            .with_delimiter(b',')
            .with_dtypes_slice(Option::Some(&dtypes))
            .finish()
            .unwrap();
        df = df.vstack(&df1).unwrap();

    }
    let now = Instant::now();

    let mut unique_activities: Vec<&str> = Vec::<&str>::with_capacity(df["concept:name"].n_unique().unwrap());
    let binding = df["concept:name"].unique()
        .unwrap()
        .rechunk();
    binding
        .iter()
        .for_each(|val| {
            if let AnyValue::Utf8(s) = val {
                unique_activities.push(s);
            }
        });
    let mut log = event_log::EventLog::new();
    let mut dfg: Dfg = Dfg { 
                tuples: Vec::<Vec<(DfgActivities, i64)>>::new(), 
                ret: AHashMap::<DfgActivities, Vec<i64>>::new()
            };
    log.create_event_log(&mut df);

    dfg.tuples.reserve(log.traces.len());
    
    dfg.tuples.par_extend((0..log.traces.len())
        .into_par_iter()
        .map(|_| Vec::<(DfgActivities, i64)>::new()));

    dfg.tuples.par_iter_mut()
        .enumerate()
        .for_each(|(idx, tuples)| {
            for i in 1..log.traces[idx].events.len() {
                let act1 = log.traces[idx].events[i-1].get("concept:name").unwrap();
                let act1_str;
                if let AnyValue::Utf8(s) = act1 {
                    act1_str = *s;
                } else {
                    act1_str = "NOT A STRING";
                };
                let act2 =  log.traces[idx].events[i].get("concept:name").unwrap();
                let act2_str;
                if let AnyValue::Utf8(s) = act2 {
                    act2_str = *s;
                } else {
                    act2_str = "NOT A STRING";
                };
                
                let activities: DfgActivities = DfgActivities { 
                    act1: act1_str,
                    act2: act2_str
                };
                
                tuples.push((
                    (activities),
                    std::cmp::max(0, (
                        NaiveDateTime::from(log.traces[idx].events[i].get("time:timestamp").unwrap()) - 
                        NaiveDateTime::from(log.traces[idx].events[i-1].get("time:timestamp").unwrap())
                    ).num_seconds())
                ));
            }
        });
    //println!("{:?}", dfg.tuples);
    //println!("{:?}", dfg.tuples.len());

    
    //println!("{}", tuples.len());
    //println!("{:?}", log.dfg.tuples);
    let mut ret: AHashMap<DfgActivities, Vec<i64>> = AHashMap::<DfgActivities, Vec<i64>>::new();

    for el in dfg.tuples {
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

    let mut end_activities: AHashMap<&str, i64> = AHashMap::<&str, i64>::new();
    let default_traceid_key:&str = "concept:name"; 
    log.traces.iter()
        .for_each(|trace| {
            let last_event = trace.events.len()-1;
            if trace.events[last_event].contains_key(default_traceid_key) {
                let activity_last_event = &trace.events[last_event][default_traceid_key];
                
                if let AnyValue::Utf8(s) = activity_last_event {
                    if !end_activities.contains_key(*s) {
                        end_activities.insert(*s, 1);
                    } else {
                        let val = end_activities.get(*s).unwrap();
                        end_activities.insert(*s, *val + 1);
                    }
                }
            }
        });
    let mut start_activities: AHashMap<&str, i64> = AHashMap::<&str, i64>::new();
    log.traces.iter()
        .for_each(|trace| {
            if trace.events[0].contains_key(default_traceid_key) {
                let activity_last_event = &trace.events[0][default_traceid_key];
                
                if let AnyValue::Utf8(s) = activity_last_event {
                    if !start_activities.contains_key(*s) {
                        start_activities.insert(*s, 1);
                    } else {
                        let val = start_activities.get(*s).unwrap();
                        start_activities.insert(*s, *val + 1);
                    }
                }
            }
        });

    
    ret.values_mut()
        .par_bridge()
        .into_par_iter()
        .for_each(|val| {
            let mut temp_vec = Vec::<i64>::with_capacity(1);
            temp_vec.push(median(val));
            *val = temp_vec;
        });
    //for v in ret.values_mut() {
    //    let mut temp_vec = Vec::<i64>::with_capacity(1);
    //    temp_vec.push(median(v));
    //    *v = temp_vec;
    //}
    visualize_dfg(&mut ret, &start_activities, &end_activities, &unique_activities, &now);
    
    //println!("{:?}", ret);

    //println!("{}", tuples[0][0].0.act1);
    //println!("{}", tuples[0][0].0.act2);
    //println!("{}", tuples[0][0].1);
}

fn visualize_dfg(dfg: &mut AHashMap::<DfgActivities, Vec<i64>>, start_activities: &AHashMap<&str, i64>, 
    end_activities: &AHashMap<&str, i64>, unique_activities: &Vec<&str>, now: &Instant) 
{
    let mut activities_count: AHashMap<&str, i64> = AHashMap::<&str, i64>::new();
    unique_activities.iter()
        .for_each(|act| {
            activities_count.insert(act, 0);
        });
    dfg.iter()
        .for_each(|el| {
            let val = activities_count.get(el.0.act2).unwrap();
            activities_count.insert(el.0.act2, val + el.1[0]);
        });
    
    start_activities.iter()
        .for_each(|act| {
            let val = activities_count.get(*act.0).unwrap();
            activities_count.insert(*act.0, val + *act.1);
        });

    
    let mut dfg_key_value_list: Vec<(DfgActivities, i64)> = Vec::<(DfgActivities, i64)>::with_capacity(dfg.len());
    dfg.iter()
        .for_each(|el| {
            dfg_key_value_list.push((*el.0, el.1[0]));
        });
    
    dfg_key_value_list.par_sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then(b.0.act1.cmp(&a.0.act1))
            .then(b.0.act2.cmp(&a.0.act2))
    });

    //this is to limit number of edges.
    let to_remove = dfg_key_value_list.drain(std::cmp::min(dfg_key_value_list.len(), 30) ..);
    to_remove.for_each(|val| {
        dfg.remove(&val.0);
    });

    let penwidth: AHashMap<DfgActivities, String> = assign_penwidth_edges(dfg);

    let mut activities_in_dfg: AHashSet<&str> = AHashSet::<&str>::new();
    dfg.iter()
        .for_each(|edge| {
            activities_in_dfg.insert(edge.0.act1);
            activities_in_dfg.insert(edge.0.act2);
        });

    let activities_color: AHashMap<&str, String> = get_activities_color(&mut activities_count);
    
    let mut g = graphviz_rust::parse(r#"
        digraph {
            graph [bgcolor=transparent]
        }
    "#).unwrap();

    g.add_stmt(stmt!(
        node!("node";
            NodeAttributes::shape(shape::box_))
    ));

    let mut hasher = DefaultHasher::new();
    let mut activities_map: AHashMap<&str, String> = AHashMap::<&str, String>::new(); 
    let mut sorted_activities_in_dfg = activities_in_dfg.into_vec();
    sorted_activities_in_dfg.par_sort();
    //println!("{:?}", sorted_activities_in_dfg);
    sorted_activities_in_dfg.iter()
        .for_each(|act| {
            act.hash(&mut hasher);
            let hash = hasher.finish();
            g.add_stmt(stmt!(
                node!(hash.to_string();
                    attr!("label", "\"".to_owned() + act + " (" + activities_count.get(act.as_str()).unwrap().to_string().as_str() + ")" + "\""),
                    attr!("style", "filled"),
                    attr!("fillcolor", "\"".to_owned() + activities_color.get(act.as_str()).unwrap() + "\""),
                    attr!("fontsize", 12))
            ));
            activities_map.insert(act.as_str(), hash.to_string());
        });
    let keys = &mut dfg.keys();
    let mut dfg_edges = keys.by_ref().collect::<Vec<&DfgActivities>>();
    dfg_edges.par_sort();

    dfg_edges.iter()
        .for_each(|edge| {
            let hash1 = activities_map.get(edge.act1).unwrap();
            let hash2 = activities_map.get(edge.act2).unwrap();
            g.add_stmt(stmt!(
                edge!(node_id!(hash1.to_string()) => node_id!(hash2.to_string());
                    attr!("label", dfg.get(edge).unwrap()[0]),
                    attr!("penwidth", penwidth.get(edge).unwrap()),
                    attr!("fontsize", 12)
                )
            ))
        });

    let mut start_activities_to_include: Vec<&str> = Vec::<&str>::new();
    start_activities.iter()
        .for_each(|act| {
            if activities_map.contains_key(*act.0) {
                start_activities_to_include.push(act.0);
            }
        });
    let mut end_activities_to_include: Vec<&str> = Vec::<&str>::new();
    end_activities.iter()
        .for_each(|act| {
            if activities_map.contains_key(*act.0) {
                end_activities_to_include.push(act.0);
            }
        });


    if start_activities_to_include.len() > 0 {
        g.add_stmt(stmt!(
            node!(("\"".to_string() + "@@startnode" + "\"");
                attr!("label", "<&#9679;>"),
                attr!("shape", "circle"),
                attr!("fontsize", 34)
            )
        ));
        start_activities_to_include.iter()
            .for_each(|act| {
                g.add_stmt(stmt!(
                    edge!(node_id!("\"".to_string() + "@@startnode" + "\"") => node_id!(activities_map.get(act).unwrap());
                        attr!("label", start_activities.get(act).unwrap().to_string()),
                        attr!("fontsize", 12)
                    )
                ))
            });
    }

    if end_activities_to_include.len() > 0 {
        g.add_stmt(stmt!(
            node!(("\"".to_string() + "@@endnode" + "\"");
                attr!("label", "<&#9632;>"),
                attr!("shape", "doublecircle"),
                attr!("fontsize", 32)
            )
        ));
        end_activities_to_include.iter()
            .for_each(|act| {
                g.add_stmt(stmt!(
                    edge!(node_id!(activities_map.get(act).unwrap()) => node_id!("\"".to_string() + "@@endnode" + "\"");
                        attr!("label", end_activities.get(act).unwrap().to_string()),
                        attr!("fontsize", 12)
                    )
                ))
            });
    }

    g.add_stmt(stmt!(
        attr!("overlap", "false")
    ));

    //println!("{:?}", g.clone().print(&mut PrinterContext::default()));
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    let mut ctx = PrinterContext::new(false, 4, "\n".to_string(), 500);
    
    let out = exec(g, &mut ctx, vec![
            CommandArg::Format(Format::Svg),
            CommandArg::Output("test.svg".to_string()),
            CommandArg::Layout(graphviz_rust::cmd::Layout::Dot),
        ]).unwrap();
}

fn assign_penwidth_edges<'a>(dfg: &'a AHashMap::<DfgActivities<'a>, Vec<i64>>) -> AHashMap<DfgActivities<'a>, String> {
    let mut penwidth: AHashMap<DfgActivities, String> = AHashMap::<DfgActivities, String>::new();

    let mut min_value = 9999999999;
    let mut max_value = -1;
    
    dfg.iter()
        .for_each(|edge| {
            if edge.1[0] < min_value {
                min_value = edge.1[0];
            }
            if edge.1[0] > max_value {
                max_value = edge.1[0];
            }
        });

    let min_edge_penwidth_graphviz = 1.0;
    let max_edge_penwidth_graphviz = 2.6;
    
    dfg.iter()
        .for_each(|edge| {
            let v0 = edge.1[0];
            let arc_penwidth: String = (min_edge_penwidth_graphviz + (max_edge_penwidth_graphviz - min_edge_penwidth_graphviz) * 
                (edge.1[0] as f64 - min_value as f64) / (max_value as f64 - min_value as f64 + 0.00001))
                    .to_string();
            penwidth.insert(*edge.0, arc_penwidth);
        });

    return penwidth;
}

fn get_activities_color<'a>(activities_count: &mut AHashMap<&'a str, i64>) -> AHashMap<&'a str, String>{
    let mut activities_color: AHashMap<&str, String> = AHashMap::<&str, String>::new();
   
    let mut min_value: i64 = 9999999999;
    let mut max_value: i64 = -1;
    
    activities_count.iter()
        .for_each(|act| {
            if *act.1 < min_value {
                min_value = *act.1;
            }
            if *act.1  > max_value {
                max_value = *act.1;
            }
        });

    
    
    activities_count.iter()
        .for_each(|act| {
            let v0 = activities_count[act.0];
            let divider = max_value as f64 - min_value as f64 + 0.00001 ;

            let trans_base_color = 255.0 - 100.0  * (v0 as f64  - min_value as f64)  /
                divider;
            let trans_base_color_hex = format!("{:02X}", trans_base_color as i64);
            let mut color: String = "#".to_owned();
            color.push_str(&trans_base_color_hex);
            color.push_str(&trans_base_color_hex);
            color.push_str("FF");
            activities_color.insert(act.0, color);
        });
    
    return activities_color;
}


fn median(values: &mut Vec<i64>) -> i64 {
    if values.len() == 0 {
        return 0;
    }
    values.sort();
    let len = values.len();
    if len % 2 == 1 {
        let middle : usize = len/2;
        return values[middle];

    } else {
        let middle: usize = len/2;
        return (values[middle-1] + values[middle])/2
    }
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


