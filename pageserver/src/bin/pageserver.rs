//
// Main entry point for the Page Server executable
//

use log::*;
use std::{fs::File, str::FromStr};
use std::io;
use std::path::PathBuf;
use std::thread;

use clap::{App, Arg};
use daemonize::Daemonize;

use slog;
use slog_stdlog;
use slog_scope;
use slog::Drain;

use pageserver::page_service;
use pageserver::restore_s3;
use pageserver::tui;
use pageserver::walreceiver;
use pageserver::walredo;
use pageserver::PageServerConf;

fn main() -> Result<(), io::Error> {
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .arg(Arg::with_name("datadir")
                 .short("D")
                 .long("dir")
                 .takes_value(true)
                 .help("Path to the page server data directory"))
        .arg(Arg::with_name("wal_producer")
                 .short("w")
                 .long("wal-producer")
                 .takes_value(true)
                 .help("connect to the WAL sender (postgres or wal_acceptor) on connstr (default: 'host=127.0.0.1 port=65432 user=zenith')"))
        .arg(Arg::with_name("listen")
                 .short("l")
                 .long("listen")
                 .takes_value(true)
                 .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5430)"))
        .arg(Arg::with_name("interactive")
                 .short("i")
                 .long("interactive")
                 .takes_value(false)
                 .help("Interactive mode"))
        .arg(Arg::with_name("daemonize")
                 .short("d")
                 .long("daemonize")
                 .takes_value(false)
                 .help("Run in the background"))
        .arg(Arg::with_name("skip_recovery")
                 .long("skip-recovery")
                 .takes_value(false)
                 .help("Skip S3 recovery procedy and start empty"))
        .get_matches();

    let mut conf = PageServerConf {
        data_dir: PathBuf::from("./"),
        daemonize: false,
        interactive: false,
        wal_producer_connstr: String::from_str("host=127.0.0.1 port=65432 user=zenith").unwrap(),
        listen_addr: "127.0.0.1:5430".parse().unwrap(),
        skip_recovery: false,
    };

    if let Some(dir) = arg_matches.value_of("datadir") {
        conf.data_dir = PathBuf::from(dir);
    }

    if arg_matches.is_present("daemonize") {
        conf.daemonize = true;
    }

    if arg_matches.is_present("interactive") {
        conf.interactive = true;
    }

    if conf.daemonize && conf.interactive {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--daemonize is not allowed with --interactive: choose one",
        ));
    }

    if arg_matches.is_present("skip_recovery") {
        conf.skip_recovery = true;
    }

    if let Some(addr) = arg_matches.value_of("wal_producer") {
        conf.wal_producer_connstr = String::from_str(addr).unwrap();
    }

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse().unwrap();
    }

    start_pageserver(conf)
}

fn start_pageserver(conf: PageServerConf) -> Result<(), io::Error> {
    // Initialize logger
    let _scope_guard = init_logging(&conf);
    let _log_guard = slog_stdlog::init().unwrap();

    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    let tui_thread: Option<thread::JoinHandle<()>>;
    if conf.interactive {
        // Initialize the UI
        tui_thread = Some(
            thread::Builder::new()
                .name("UI thread".into())
                .spawn(|| {
                    let _ = tui::ui_main();
                })
                .unwrap(),
        );
        //threads.push(tui_thread);
    } else {
        tui_thread = None;
    }

    if conf.daemonize {
        info!("daemonizing...");

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fpritf's or backtraces.
        let stdout = File::create(conf.data_dir.join("pageserver.log")).unwrap();
        let stderr = File::create(conf.data_dir.join("pageserver.log")).unwrap();

        let daemonize = Daemonize::new()
            .pid_file(conf.data_dir.join("pageserver.pid"))
            .working_directory(conf.data_dir.clone())
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

    let mut threads = Vec::new();

    info!("starting...");

    // Initialize the WAL applicator
    let walredo_thread = thread::Builder::new()
        .name("WAL redo thread".into())
        .spawn(|| {
            walredo::wal_applicator_main();
        })
        .unwrap();
    threads.push(walredo_thread);

    // Before opening up for connections, restore the latest base backup from S3.
    // (We don't persist anything to local disk at the moment, so we need to do
    // this at every startup)
    if !conf.skip_recovery {
        restore_s3::restore_main();
    }

    // Launch the WAL receiver thread. It will try to connect to the WAL safekeeper,
    // and stream the WAL. If the connection is lost, it will reconnect on its own.
    // We just fire and forget it here.
    let conf1 = conf.clone();
    let walreceiver_thread = thread::Builder::new()
        .name("WAL receiver thread".into())
        .spawn(|| {
            // thread code
            walreceiver::thread_main(conf1);
        })
        .unwrap();
    threads.push(walreceiver_thread);

    // GetPage@LSN requests are served by another thread. (It uses async I/O,
    // but the code in page_service sets up it own thread pool for that)
    let conf2 = conf.clone();
    let page_server_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(|| {
            // thread code
            page_service::thread_main(conf2);
        })
        .unwrap();
    threads.push(page_server_thread);

    if tui_thread.is_some() {
        // The TUI thread exits when the user asks to Quit.
        tui_thread.unwrap().join().unwrap();
    } else {
        // In non-interactive mode, wait forever.
        for t in threads {
            t.join().unwrap()
        }
    }
    Ok(())
}

fn init_logging(conf: &PageServerConf) -> slog_scope::GlobalLoggerGuard {
    if conf.interactive {
        tui::init_logging()
    } else if conf.daemonize {
        let log = conf.data_dir.join("pageserver.log");
        let log_file = File::create(log).unwrap_or_else(|_| panic!("Could not create log file"));
        let decorator = slog_term::PlainSyncDecorator::new(log_file);
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        slog_scope::set_global_logger(logger)
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
        let drain = slog::Filter::new(drain, |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Info) {
                return true;
            }
            if record.level().is_at_least(slog::Level::Debug)
                && record.module().starts_with("pageserver")
            {
                return true;
            }
            return false;
        })
        .fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        slog_scope::set_global_logger(logger)
    }
}
