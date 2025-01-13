use crate::common::*;

// 제네릭 구조체 정의
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct ScoredData<T> {
    pub score: i32,
    pub data: T,
}

// 점수를 기준으로 정렬
#[derive(Eq, PartialEq)]
struct MinHeapItem(i32);

impl Ord for MinHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // 낮은 점수 우선 정렬 (min-heap)
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for MinHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct ScoreManager<T> {
    heap: BinaryHeap<MinHeapItem>,              // 점수를 최소 힙으로 관리
    data_map: HashMap<i32, Vec<ScoredData<T>>>, // 점수별 데이터 관리
}

impl<T> ScoreManager<T> {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            data_map: HashMap::new(),
        }
    }

    // 점수와 데이터를 삽입
    pub fn insert(&mut self, score: i32, data: T) {
        // 데이터 삽입
        self.data_map
            .entry(score)
            .or_insert_with(Vec::new)
            .push(ScoredData { score, data });

        // 힙에 점수를 추가 (이미 존재하는 점수도 중복 삽입 가능)
        if !self.heap.iter().any(|MinHeapItem(s)| *s == score) {
            self.heap.push(MinHeapItem(score));
        }
    }

    // 가장 낮은 점수와 데이터를 가져오기
    pub fn pop_lowest(&mut self) -> Option<ScoredData<T>> {
        // 힙에서 가장 낮은 점수 가져오기
        let lowest_score = self.heap.pop()?.0;

        // 해당 점수의 데이터 목록에서 하나를 꺼냄
        if let Some(mut data_list) = self.data_map.remove(&lowest_score) {
            let result = data_list.pop();

            // 데이터가 남아 있으면 다시 삽입
            if !data_list.is_empty() {
                self.data_map.insert(lowest_score, data_list);
                self.heap.push(MinHeapItem(lowest_score)); // 점수를 다시 힙에 추가
            }

            return result;
        }

        None
    }
}
