function (key, values, rereduce) {
  // Want to sum only the file / size
  // information. Use this JSON object
  // to track what we want summed
  ret = {'good' : 0, 'staged_bytes' : 0,
         'failed' : 0, 'failed_bytes' : 0,
         'incomplete' : 0, 'incomplete_bytes' : 0};

  // Timestamps are min / max - track here
  // and add to JSON at end
  var mintime = 0;
  var maxtime = 0;

  for(entry in values)
  {
    var req = values[entry];

    // Sum required values
    for(key in ret)
    {
      ret[key] += req[key];
    }

    // Process min / max times
    var curmin;
    var curmax;
    if(rereduce)
    {
      curmin = req['time_start'];
      curmax = req['time_end'];
    }
    else
    {
      curmin = req['stage_timestamp'];
      curmax = curmin;
    }
    if(mintime == 0)
    {
      mintime = curmin;
    }
    else
    {
      mintime = Math.min(mintime, curmin);
    }
    maxtime = Math.max(maxtime, curmax);
  }

  // Add times to document
  ret['time_start'] = mintime;
  ret['time_end'] = maxtime;

  return ret;
}

