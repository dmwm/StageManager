function(doc) {
  //Long winded way of getting the keys from a dictionary
  var req_id;
  for (req_id in doc.results){//loop over all requests in a doc
    emit(['staged_bytes',req_id], doc.results[req_id].staged_bytes);
    emit(['incomplete_bytes',req_id], doc.results[req_id].incomplete_bytes);
    emit(['failed_bytes',req_id], doc.results[req_id].failed_bytes);
  }
}
