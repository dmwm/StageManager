function(doc) {
    var req_id;
    for (req_id in doc.results){//loop over all requests in a doc
	emit(['no_of_staged_files',req_id], doc.results[req_id].good);
	emit(['no_of_incomplete_files',req_id], doc.results[req_id].incomplete);
	emit(['no_of_failed_files',req_id], doc.results[req_id].failed);
    }
}