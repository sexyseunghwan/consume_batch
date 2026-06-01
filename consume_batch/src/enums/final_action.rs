use crate::enums::IndexingType;

/// Batch 내 동일 key의 이벤트들을 압축한 최종 작업 단위.
///
/// 상태 전이는 [`FinalAction::apply_event`]를 통해 이루어진다.
/// - `UpsertNew`     : 이 배치에서 처음 생성된 문서 (Insert가 첫 이벤트).
///                     이후 Delete 이벤트가 오면 ES에 존재하지 않으므로 Noop.
/// - `UpsertExisting`: 이 배치 이전부터 존재하던 문서 (Update/Delete→Insert가 첫 이벤트).
///                     이후 Delete 이벤트가 오면 ES에서 실제로 삭제해야 하므로 Delete.
/// - `Delete`        : ES에서 해당 문서를 삭제해야 함.
/// - `Noop`          : 이 배치 내에서 생성 후 삭제 — ES에 변경 없음.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalAction {
    UpsertNew,
    UpsertExisting,
    Delete,
    Noop,
}

impl FinalAction {
    pub fn apply_event(self, event: IndexingType) -> Self {
        match (self, event) {
            // UpsertNew: I가 첫 이벤트였던 상태
            (Self::UpsertNew, IndexingType::Insert) => Self::UpsertNew,
            (Self::UpsertNew, IndexingType::Update) => Self::UpsertNew,
            (Self::UpsertNew, IndexingType::Delete) => Self::Noop, // I→D: 생성 후 즉시 삭제 → no-op

            // UpsertExisting: U 또는 D→I가 첫 이벤트였던 상태
            (Self::UpsertExisting, IndexingType::Insert) => Self::UpsertExisting,
            (Self::UpsertExisting, IndexingType::Update) => Self::UpsertExisting,
            (Self::UpsertExisting, IndexingType::Delete) => Self::Delete, // U→D: 기존 문서 삭제

            // Delete: 삭제 예정 상태
            (Self::Delete, IndexingType::Insert) => Self::UpsertExisting, // D→I: 재생성
            (Self::Delete, IndexingType::Update) => Self::UpsertExisting,
            (Self::Delete, IndexingType::Delete) => Self::Delete,

            // Noop: I→D 로 아무 작업도 없는 상태
            (Self::Noop, IndexingType::Insert) => Self::UpsertNew, // I→D→I: 재생성
            (Self::Noop, IndexingType::Update) => Self::UpsertNew,
            (Self::Noop, IndexingType::Delete) => Self::Noop,
        }
    }

    pub fn initialize_from_first_event(event: IndexingType) -> Self {
        match event {
            IndexingType::Insert => Self::UpsertNew,
            IndexingType::Update => Self::UpsertExisting,
            IndexingType::Delete => Self::Delete,
        }
    }

    pub fn is_upsert_required(&self) -> bool {
        matches!(self, Self::UpsertNew | Self::UpsertExisting)
    }
}
