/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

use std::{
    fs::{File, OpenOptions},
    io, thread, time,
};

use reqwest::blocking::Client;
use spin::Mutex;

use crate::{
    error::{CommonErrCode, Error::Common as CommonErr, Result},
    location::ObServerAddr,
    util::current_time_millis,
};

struct OcpModelCacheFile {
    ocp_model: Mutex<Option<OcpModel>>,
    path: String,
}

const SAVE_SLOW_THRESHOLD_MS: i64 = 100;

impl OcpModelCacheFile {
    fn new(path: &str) -> OcpModelCacheFile {
        let file = OcpModelCacheFile {
            path: path.to_owned(),
            ocp_model: Mutex::new(None),
        };
        if let Err(e) = file.load() {
            debug!(
                "OcpModelCacheFile::new fail to load cached ocp model, err: {}",
                e
            );
        }
        file
    }

    fn load(&self) -> Result<OcpModel> {
        let file = File::open(&self.path)?;
        let reader = io::BufReader::new(file);
        let model: OcpModel = serde_json::from_reader(reader)?;
        *self.ocp_model.lock() = Some(model.clone());

        Ok(model)
    }

    fn save(&self, model: &OcpModel) -> Result<()> {
        let mut current_model = self.ocp_model.lock();
        if let Some(ref m) = *current_model {
            if m == model {
                return Ok(());
            }
        }

        let start = current_time_millis();

        *current_model = Some(model.to_owned());
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, model)?;

        let cost = current_time_millis() - start;
        if cost > SAVE_SLOW_THRESHOLD_MS {
            warn!("OcpModelCacheFile::save cost {} ms.", cost);
        }

        Ok(())
    }

    fn ocp_model(&self) -> Option<OcpModel> {
        self.ocp_model.lock().clone()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct OcpModel {
    pub observer_addrs: Vec<ObServerAddr>,
    pub cluster_id: i64,
}

impl OcpModel {
    pub fn new() -> Self {
        Self {
            cluster_id: 0,
            observer_addrs: vec![],
        }
    }
}

impl Default for OcpModel {
    fn default() -> OcpModel {
        OcpModel::new()
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
struct OcpResponseDataRs {
    address: String,
    role: String,
    sql_port: i32,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
struct OcpResponseData {
    #[serde(rename = "ObRegion")]
    ob_region: String,
    #[serde(rename = "ObRegionId")]
    ob_region_id: i64,
    #[serde(rename = "RsList")]
    rs_list: Vec<OcpResponseDataRs>,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
struct OcpResponse {
    #[serde(rename = "Code")]
    code: i32,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "Success")]
    success: bool,
    #[serde(rename = "Data")]
    data: OcpResponseData,
}

impl Default for OcpResponseDataRs {
    fn default() -> OcpResponseDataRs {
        OcpResponseDataRs {
            address: "".to_owned(),
            role: "".to_owned(),
            sql_port: 0,
        }
    }
}

impl Default for OcpResponseData {
    fn default() -> OcpResponseData {
        OcpResponseData {
            ob_region: "".to_owned(),
            ob_region_id: -1,
            rs_list: vec![],
        }
    }
}

impl Default for OcpResponse {
    fn default() -> OcpResponse {
        OcpResponse {
            code: 0,
            message: "".to_owned(),
            success: false,
            data: OcpResponseData::default(),
        }
    }
}

pub struct ObOcpModelManager {
    client: Client,
    cache_file: OcpModelCacheFile,
}

impl ObOcpModelManager {
    pub fn new(http_timeout: time::Duration, cache_path: &str) -> Result<ObOcpModelManager> {
        Ok(Self {
            client: Client::builder().timeout(http_timeout).build()?,
            cache_file: OcpModelCacheFile::new(cache_path),
        })
    }

    fn load_ocp_model_once(&self, param_url: &str, datasource_name: &str) -> Result<OcpModel> {
        match self.client.get(param_url).send()?.text() {
            Ok(text) => {
                let response: std::result::Result<OcpResponse, _> = serde_json::from_str(&text);
                if let Ok(response) = response {
                    if response.success && response.code == 200 {
                        if !datasource_name.is_empty() {
                            //TODO
                            //save local context
                        }
                        let mut ret = OcpModel::new();
                        let data = response.data;

                        ret.cluster_id = data.ob_region_id;

                        for rs in data.rs_list {
                            let mut addr = ObServerAddr::new();

                            addr.address(rs.address);
                            addr.set_sql_port(rs.sql_port);
                            ret.observer_addrs.push(addr);
                        }

                        if ret.observer_addrs.is_empty() {
                            Err(CommonErr(
                                CommonErrCode::OcpError,
                                "Empty rs list".to_owned(),
                            ))
                        } else {
                            // Save the result into cache file.
                            if let Err(e) = self.cache_file.save(&ret) {
                                error!("OcpModelCacheFile::load_ocp_model_once fail to save ocp model to cache file: {}, err: {}",
                                       self.cache_file.path, e);
                            }

                            Ok(ret)
                        }
                    } else {
                        error!(
                            "ocp::load_ocp_model fail to fetch ocp model, the response is : {:?}",
                            response
                        );
                        Err(CommonErr(
                            CommonErrCode::OcpError,
                            "Invalid response.".to_owned(),
                        ))
                    }
                } else {
                    error!(
                        "ocp::load_ocp_model fail to fetch ocp model, the response is : {:?}",
                        response
                    );
                    Err(CommonErr(
                        CommonErrCode::OcpError,
                        "Invalid response.".to_owned(),
                    ))
                }
            }
            Err(e) => {
                error!(
                    "ocp::load_ocp_model fail to fetch ocp model, the err is : {:?}",
                    e
                );
                Err(CommonErr(
                    CommonErrCode::OcpError,
                    "Http fetch error.".to_owned(),
                ))
            }
        }
    }

    pub fn load_ocp_model(
        &self,
        param_url: &str,
        datasource_name: &str,
        retry_times: usize,
        retry_interval: time::Duration,
        from_cache_when_fail: bool,
    ) -> Result<OcpModel> {
        for _ in 0..retry_times {
            let ret = self.load_ocp_model_once(param_url, datasource_name);
            if ret.is_ok() {
                return ret;
            } else {
                thread::sleep(retry_interval);
            }
        }

        // Try to load from cache.
        if from_cache_when_fail {
            if let Some(model) = self.cache_file.ocp_model() {
                return Ok(model);
            }
        }

        Err(CommonErr(
            CommonErrCode::OcpError,
            "Fail to load OcpModel too many times".to_owned(),
        ))
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tempfile::Builder;

    use super::*;

    // TODO(xikai): use test conf to control which environments to test.
    const TEST_URL: &str = "127.0.0.1";

    #[test]
    #[ignore = "need to start ocp server"]
    fn test_load_ocp_model() {
        let manager = ObOcpModelManager::new(Duration::from_secs(10), "/tmp/test")
            .expect("fail to create ocp manager.");
        let model = manager
            .load_ocp_model(TEST_URL, "", 3, Duration::from_secs(1), false)
            .expect("Fail to load ocp model");

        assert_eq!(model.cluster_id, 1774840318);
        assert_eq!(3, model.observer_addrs.len());
    }

    #[test]
    fn test_cache_ocp_model() {
        let dir = Builder::new().prefix("ocp_model").tempfile().unwrap();
        let path = dir.path().to_str().unwrap();

        let file = OcpModelCacheFile::new(path);

        assert!(!file.load().is_ok());

        let mut ocp_model = OcpModel::new();
        for i in 0..3 {
            let mut addr = ObServerAddr::new();

            addr.address("test_host".to_owned());
            addr.set_sql_port(i);
            ocp_model.observer_addrs.push(addr);
        }

        file.save(&ocp_model).expect("Fail to save model");

        let model = file.load().expect("Fail to load ocp model");
        assert_eq!(ocp_model, model);

        // load again
        let model = file.load().expect("Fail to load ocp model");
        assert_eq!(ocp_model, model);
    }
}
