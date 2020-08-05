use once_cell::sync::Lazy;
use std::sync::{Arc, RwLock};

const PATH: &'static str = "./test";

static RKV: Lazy<Arc<RwLock<rkv::Rkv<rkv::backend::LmdbEnvironment>>>> = Lazy::new(|| {
    use rkv::backend::{
        BackendEnvironmentBuilder,
        BackendEnvironmentFlags,
    };

    std::fs::create_dir_all(PATH).unwrap();

    let mut manager = rkv::Manager::<rkv::backend::LmdbEnvironment>::singleton()
        .write()
        .unwrap();

    let path = std::path::Path::new(PATH);

    let rkv = manager.get_or_create(path, |path| {
        let mut env_bld = rkv::Rkv::environment_builder::<rkv::backend::Lmdb>();
        let mut flags = rkv::backend::LmdbEnvironmentFlags::default();
        flags.set(rkv::EnvironmentFlags::WRITE_MAP, true);
        flags.set(rkv::EnvironmentFlags::MAP_ASYNC, true);
        flags.set(rkv::EnvironmentFlags::NO_TLS, true);
        env_bld
            .set_map_size(100 * 1024 * 1024)
            .set_max_dbs(32)
            .set_flags(flags);
        rkv::Rkv::from_builder::<rkv::backend::Lmdb>(path, env_bld)
    }).unwrap();

    let mut opts = rkv::store::Options::create();
    opts.create = true;

    // ensure our database is created with create=true (cannot be parallel)
    // later we will access them with create=false
    let _db1 = rkv.read().unwrap().open_single("ZOMBIE", opts).unwrap();

    rkv
});

struct ZLock<'lt>(*mut std::sync::RwLockReadGuard<'lt, rkv::Rkv<rkv::backend::LmdbEnvironment>>);

impl Drop for ZLock<'_> {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.0));
        }
    }
}

pub struct ZombieRead<'lt> {
    _lock: ZLock<'lt>,
    db: rkv::store::single::SingleStore<rkv::backend::LmdbDatabase>,
    rdr: rkv::Reader<rkv::backend::LmdbRoTransaction<'lt>>,
}

impl ZombieRead<'_> {
    pub fn new() -> Self {
        let mut opts = rkv::store::Options::create();
        // this can be done in parallel
        opts.create = false;

        let lock = Box::into_raw(Box::new(RKV.read().unwrap()));

        let (db, rdr) = unsafe {
            (
                lock.as_ref().unwrap().open_single("ZOMBIE", opts).unwrap(),
                lock.as_ref().unwrap().read().unwrap(),
            )
        };

        Self {
            _lock: ZLock(lock),
            db,
            rdr,
        }
    }

    pub fn get(&self, k: &[u8]) -> Vec<u8> {
        self.db.get(&self.rdr, k).unwrap().map(|v| {
            match v {
                rkv::Value::Blob(v) => v.to_vec(),
                _ => panic!("bad lmdb value type: {:?}", v),
            }
        }).unwrap()
    }
}

pub struct ZombieWrite<'lt> {
    _lock: ZLock<'lt>,
    db: rkv::store::single::SingleStore<rkv::backend::LmdbDatabase>,
    wtr: rkv::Writer<rkv::backend::LmdbRwTransaction<'lt>>,
}

impl ZombieWrite<'_> {
    pub fn new() -> Self {
        let mut opts = rkv::store::Options::create();
        // this can be done in parallel
        opts.create = false;

        let lock = Box::into_raw(Box::new(RKV.read().unwrap()));

        let (db, wtr) = unsafe {
            (
                lock.as_ref().unwrap().open_single("ZOMBIE", opts).unwrap(),
                lock.as_ref().unwrap().write().unwrap(),
            )
        };

        Self {
            _lock: ZLock(lock),
            db,
            wtr,
        }
    }

    pub fn commit(self) {
        let Self { _lock, wtr, .. } = self;
        wtr.commit().unwrap();
    }

    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        let _ = self.db.delete(&mut self.wtr, k);
        self.db.put(&mut self.wtr, k, &rkv::Value::Blob(v)).unwrap();
    }
}

const ENTRY_SIZE: usize = 400;

fn stat() {
    RKV.read().unwrap().sync(true).unwrap();
    let _ = std::process::Command::new("mdb_stat")
        .args(&["-efffs", "ZOMBIE", PATH])
        .status()
        .unwrap();
}

fn full_rewrite(byte: u8) {
    let mut writer = ZombieWrite::new();
    let val = [byte; ENTRY_SIZE];
    for i in 0..255_u8 {
        for j in 0..255_u8 {
            writer.put(&[i, j], &val);
        }
    }
    writer.commit();
}

fn exec_test(keep_readers: bool) {
    let hint = if keep_readers { "keep" } else { "drop" };

    stat();

    full_rewrite(1);
    let reader_1 = ZombieRead::new();
    println!("---------------------------");
    println!("-- read_{}: {:?}", hint, reader_1.get(&[1, 2])[0]);
    println!("---------------------------");
    if !keep_readers {
        drop(reader_1);
    }

    stat();

    full_rewrite(2);
    let reader_2 = ZombieRead::new();
    println!("---------------------------");
    println!("-- read_{}: {:?}", hint, reader_2.get(&[1, 2])[0]);
    println!("---------------------------");
    if !keep_readers {
        drop(reader_2);
    }

    stat();

    full_rewrite(3);
    let reader_3 = ZombieRead::new();
    println!("---------------------------");
    println!("-- read_{}: {:?}", hint, reader_3.get(&[1, 2])[0]);
    println!("---------------------------");
    if !keep_readers {
        drop(reader_3);
    }
}

fn main() {
    full_rewrite(255); // warm the db

    exec_test(false); // test 3 cycles, dropping readers

    exec_test(true);  // test 3 cycles, keeping readers
}
