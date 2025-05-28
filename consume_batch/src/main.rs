/*
Author      : Seunghwan Shin
Create date : 2025-01-01
Description :

History     : 2025-01-01 Seunghwan Shin       # [v.1.0.0] first create
              2025-03-22 Seunghwan Shin       # [v.1.1.0] Change the RDB-related crate (Diesel -> Sear-orm)
              2025-05-28 Seunghwan Shin       # [v.1.1.1] Correct duplicate index problem
*/

mod common;
use common::*;

mod utils_module;
use utils_module::logger_utils::*;

mod repository;

mod models;

mod controller;
use controller::main_controller::*;

mod service;
use service::es_query_service::*;
use service::query_service::*;

mod configuration;

mod entity;

#[tokio::main]
async fn main() {
    set_global_logger();
    dotenv().ok();

    info!("Batch Program Start");

    let query_service: QueryServicePub = QueryServicePub::new();
    let es_query_service: EsQueryServicePub = EsQueryServicePub::new();
    let main_controller: MainController<QueryServicePub, EsQueryServicePub> =
        MainController::new(query_service, es_query_service);

    match main_controller.main_task().await {
        Ok(_) => info!("Batch Program End"),
        Err(e) => error!("{:?}", e),
    }
}

// // 소수 판별 함수
// fn is_prime(n: u64) -> bool {
//     if n < 2 {
//         return false;
//     }
//     for i in 2..=((n as f64).sqrt() as u64) {
//         if n % i == 0 {
//             return false;
//         }
//     }
//     true
// }

// // 멀티스레드 방식
// fn find_primes_with_threads(range: Vec<u64>, num_threads: usize) -> Vec<u64> {
//     let chunk_size = (range.len() + num_threads - 1) / num_threads;
//     let mut handles = vec![];

//     for chunk in range.chunks(chunk_size) {
//         let chunk = chunk.to_vec();
//         let handle = std::thread::spawn(move || {
//             chunk.into_iter().filter(|&x| is_prime(x)).collect::<Vec<u64>>()
//         });
//         handles.push(handle);
//     }

//     let mut results = vec![];
//     for handle in handles {
//         results.extend(handle.join().unwrap());
//     }

//     results
// }

// // 단일 스레드 방식
// fn find_primes_without_threads(range: Vec<u64>) -> Vec<u64> {
//     range.into_iter().filter(|&x| is_prime(x)).collect()
// }

// fn main() {
//     let range: Vec<u64> = (1..=10_000_000).collect(); // 1부터 100,000까지
//     let num_threads = 10;

//     // 단일 스레드 실행
//     let start = Instant::now();
//     let single_thread_primes = find_primes_without_threads(range.clone());
//     let single_thread_duration = start.elapsed();

//     // 멀티스레드 실행
//     let start = Instant::now();
//     let multi_thread_primes = find_primes_with_threads(range.clone(), num_threads);
//     let multi_thread_duration = start.elapsed();

//     // 결과 비교
//     println!(
//         "Single-threaded time: {:?}, Multi-threaded time: {:?}",
//         single_thread_duration, multi_thread_duration
//     );

//     // 결과 확인 (선택 사항)
//     assert_eq!(single_thread_primes, multi_thread_primes); // 결과 동일성 확인
//     println!("Total primes found: {}", single_thread_primes.len());
// }
