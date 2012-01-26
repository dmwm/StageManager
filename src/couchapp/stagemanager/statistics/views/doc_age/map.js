function(doc) {
  emit(doc._rev, doc.stage_timestamp);
}
