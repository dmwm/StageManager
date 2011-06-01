function(doc) {
    var req_id;
    for (req_id in doc.results){//loop over all requests in a doc
	emit(['good files staged',req_id], doc.results[req_id].good);
	emit(['incomplete files staged',req_id], doc.results[req_id].incomplete);
	emit(['failed files staged',req_id], doc.results[req_id].failed);
    }
}
