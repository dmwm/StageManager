function(doc) {
  // Emit a KV pair per stage request ID
  for(request in doc.results)
  {
    // Want to return the stage request
    // summary information
    var req = doc.results[request];

    // All done
    emit(request, req);
  }
}

