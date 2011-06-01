function(doc) {
  emit(['Database submissions',doc.site], 1);
  emit('Total stage time (secs)', doc.stage_duration);
}
