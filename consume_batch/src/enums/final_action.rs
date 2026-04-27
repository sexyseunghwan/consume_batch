use crate::enums::IndexingType;

/// Batch 내 동일 key의 이벤트들을 압축한 최종 작업 단위.
///
/// 상태 전이는 [`FinalAction::merge`]를 통해 이루어진다.
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
    /// 현재 상태(self)에 새 이벤트(event)를 적용해 다음 상태를 반환한다.
    ///
    /// ## 상태 전이표
    ///
    /// | prev \ event  | Insert        | Update        | Delete  |
    /// |---------------|---------------|---------------|---------|
    /// | UpsertNew     | UpsertNew     | UpsertNew     | Noop    |
    /// | UpsertExisting| UpsertExisting| UpsertExisting| Delete  |
    /// | Delete        | UpsertExisting| UpsertExisting| Delete  |
    /// | Noop          | UpsertNew     | UpsertNew     | Noop    |
    pub fn modify_by_event(self, event: IndexingType) -> Self {
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

    /// 초기 이벤트(prev 상태 없음)로부터 첫 FinalAction을 결정한다.
    pub fn initialize_from_first_event(event: IndexingType) -> Self {
        match event {
            IndexingType::Insert => Self::UpsertNew,
            IndexingType::Update => Self::UpsertExisting,
            IndexingType::Delete => Self::Delete,
        }
    }

    /// 이 FinalAction이 ES upsert 작업이 필요한지 여부.
    pub fn is_upsert_required(&self) -> bool {
        matches!(self, Self::UpsertNew | Self::UpsertExisting)
    }
}
