function(doc) {
  emit([doc._id,doc.data], 1);
}