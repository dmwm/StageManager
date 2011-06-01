function(doc) {
  emit('Database submissions', 1);
  emit('Total stage time (secs)', doc.stage_duration);
}
