import json
import youtube_session
import argparse
import urlparse


#Reference https://www.genyoutube.net/formats-resolution-youtube-videos.html
VIDEO_QUALITY_DICT = {
  '5': 240,
  '6': 270,
  '13': 270,
  '17': 144,
  '18': 360,
  '22': 720,
  '34': 360,
  '35': 480,
  '36': 240,
  '37': 1080,
  '38': 3072,
  '43': 360,
  '44': 480,
  '45': 720,
  '46': 1080,
  '82': 360,
  '83': 480,
  '84': 720,
  '85': 1080,
  '100': 360,
  '101': 480,
  '102': 720,
  '92': 240,
  '93': 360,
  '94': 480,
  '95': 720,
  '96': 1080,
  '132': 240,
  '151': 72,
  '133': 240,
  '134': 360,
  '135': 480,
  '136': 720,
  '137': 1080,
  '138': 2160,
  '160': 144,
  '264': 1440,
  '298': 720,
  '299': 1080,
  '266': 2160,
  '139': 0,
  '140': 0,
  '141': 0,
  '167': 360,
  '168': 480,
  '169': 720,
  '170': 1080,
  '218': 480,
  '219': 144,
  '242': 240,
  '243': 360,
  '244': 480,
  '245': 480,
  '246': 480,
  '247': 720,
  '248': 1080,
  '271': 1440,
  '272': 2160,
  '302': 2160,
  '303': 1080,
  '308': 1440,
  '313': 2160,
  '315': 2160,
  '171': 0,
  '172': 0
}

def prepare_entry(entry, ys):
  try:
    request = {}
    if "googlevideo" in entry["OnBeforeRequestOptions"]['url']:
      if entry["onCompleted"]["statusCode"] > 204:
        return None

      request['ip'] = entry["onCompleted"]['ip']
      request['start_ts'] = int(entry["OnBeforeRequestOptions"]['timeStamp'])
      request['end_ts'] = int(entry["onCompleted"]['timeStamp'])
      request['url'] = entry["onCompleted"]['url']
      request['method'] = entry["onCompleted"]['method']
      request['status'] = entry["onCompleted"]['statusCode']
      request['content-type'] = ""
      for header in entry["onCompleted"]["responseHeaders"]:
        if header["name"] == 'content-type':
          request['content-type'] = header["value"]
          continue

      url = urlparse.urlparse(request['url'])
      query = urlparse.parse_qs(url.query)

      request['itag'] = query["itag"][0]
      if request['itag'] in VIDEO_QUALITY_DICT:
        request['resolution'] = VIDEO_QUALITY_DICT[request['itag']]
      else:
        request['resolution'] = 0

      request['range'] = query['range'][0]
      request['clen'] = query['clen'][0]

      request['download_start'] = {
        # "PS": "Normal",
        #                 "VMAF": "81/81",
        #                 "RS": "Playing",
        #                 "PBR": "96 / 910 (853x480)",
        #                 "Pos": "2917.436",
        #                 "BBR": "96 / 910",
        #                 "Th": "39247",
        #                 "BSe": "234.606 / 225.704",
        #                 "BB2": "26654919",
        #                 "BB1": "2927306 / 23727613"
        'closest': ys.get_timestamp_by_time(request['start_ts']),
        'buffer': ys.get_buffer_by_time(request['start_ts']),
        'pos': ys.get_currentTimes_by_time(request['start_ts']),

      }

      request['download_end'] = {
        'closest': ys.get_timestamp_by_time(request['end_ts']),
        'buffer': ys.get_buffer_by_time(request['end_ts']),
        'pos': ys.get_currentTimes_by_time(request['end_ts']),

      }

      return request

    else:
      return None

  except KeyError as key:
    # print 'Entry does not have key:', key.message
    return None
  except Exception as e:
    # print 'Some other error:',e,'\nFor entry',val
    return None

def process_session(web_history="requests_history.json", ext_file="ext.json", outfile="out.json"):
  wh = open(web_history)
  fn = json.load(wh)
  wh.close()
  ys = youtube_session.YoutubeSession(ext_file)
  out_json ={
    "movie_id": ys.mid,
    "duration": ys.duration,
    "startTs": ys.startTime,
    "endTs": ys.endTime,
    "vals": {}
  }
  for key in fn:
    entry = prepare_entry(fn[key], ys)
    if entry is None:
      continue
    out_json["vals"][key] = entry

  o = open(outfile, 'w')
  text = json.dumps(out_json, indent=4, separators=(',', ': '))
  o.write(text)
  o.close()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('-w', '--web', type=str, required=True, help="web history file")
  parser.add_argument('-e', '--ext', type=str, required=True, help="extension file")
  parser.add_argument('-o', '--out', type=str, required=True, help="outfile file")
  args = vars(parser.parse_args())

  process_session(web_history=args["web"], ext_file=args["ext"], outfile=args["out"])