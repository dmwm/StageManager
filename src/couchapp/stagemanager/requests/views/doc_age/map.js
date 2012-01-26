function(doc) {
  emit(doc._rev, doc.create_timestamp);
}
