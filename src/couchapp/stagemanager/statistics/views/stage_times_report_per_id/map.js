function(doc) {
    var req_id;
    for (req_id in doc.results){//loop over all requests in a doc
	emit(['stage_duration',req_id], doc.results[req_id].stage_duration);
        emit(['ave_stage_time_per_file',req_id], doc.results[req_id].ave_stage_time_per_file);
     }
}
