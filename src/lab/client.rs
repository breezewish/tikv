// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use hyper;
use hyper::rt::Future;
use hyper::Client;
use hyper::{Body, Method, Request};
use serde_json;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use storage::Mutation;
use tokio_core::reactor::Handle;
use util::worker::FutureRunnable;
use util::worker::FutureWorker;
use hyper::http::header::HeaderValue;
use serde::Serialize;
use serde::Serializer;

#[derive(Deserialize, Debug)]
pub enum Report {
    TiKVStarted{tikv_id: u32, addr: String},
    TiKVStopped{tikv_id: u32, addr: String},
}

impl Report {
    fn jsonify(&self) -> String
    {
        match *self {
            Report::TiKVStarted{tikv_id, ref addr} => format!("{{\"tikv_id\": {}, \"addr\": \"{}\"}}", tikv_id, addr),
            Report::TiKVStopped{tikv_id, ref addr} => format!("{{\"tikv_id\": {}, \"addr\": \"{}\"}}", tikv_id, addr),
        }
    }
}

impl Report {
    fn event_name(&self) -> String {
        match *self {
            Report::TiKVStarted{..} => "TiKVStarted".to_string(),
            Report::TiKVStopped{..} => "TiKVStopped".to_string(),
        }
    }
}

impl Display for Report {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        ::std::fmt::Debug::fmt(self, f)
    }
}

// A client send request to the lab server

pub struct LabClient {
    pub worker: FutureWorker<Report>,
}

pub struct Runner {}

impl Runner {
    pub fn new() -> Runner {
        Runner {}
    }
}

impl FutureRunnable<Report> for Runner {
    fn run(&mut self, report: Report, handle: &Handle) {
        let client = Client::new();

        let uri: hyper::Uri = format!("{}", "http://192.168.198.207:12510/event").parse().unwrap();

        let serialized = format!(
            "{}{}{}{}{}",
            "{\"event_name\": \"".to_string(),
            report.event_name().to_string(),
            "\",\"payload\": ".to_string(),
            report.jsonify(),
            "}".to_string(),
        );
        println!("serialized = {}", serialized);

        let mut req = Request::new(Body::from(serialized));

        *req.method_mut() = Method::POST;
        *req.uri_mut() = uri.clone();
        req.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json")
        );

        let f = client
            .request(req)
            .and_then(|res| {
                println!("Response: {}", res.status());
                println!("Headers: {:#?}", res.headers());

                Ok(res)
            })
            .map(|_| {
                println!("\n\nDone.");
            })
            .map_err(|err| {
                eprintln!("Error {}", err);
            });

        handle.spawn(f)
    }
}

impl LabClient {
    pub fn new() -> LabClient {
        info!("Create Lab Client for Lab Servre");
        let worker = FutureWorker::new("Lab Client");

        LabClient { worker }
    }

    pub fn start(&mut self) -> () {
        let runner = Runner::new();
        self.worker.start(runner);

        ()
    }
}
