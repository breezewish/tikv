mod client;

pub use self::client::LabClient;
pub use self::client::Report;

lazy_static! {
    pub static ref labClient: LabClient = {
    let mut c = LabClient::new();
    c.start();
    c
    };

}
