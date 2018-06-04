import json
from collections import OrderedDict


#     "ts":                               "ts",
#     "Version":                          "V",
#     "Esn":                              "ESN",
#     "PBCID":                            "PBC",
#     "UserAgent":                        "UA",
#     "MovieId":                          "MI",
#     "TrackingId":                       "TI",
#     "Xid":                              "XID",
#     "Position":                         "Pos",
#     "Duration":                         "Dur",
#     "Volume":                           "Vol",
#     "Player state":                     "PS",
#     "Buffering state":                  "BS",
#     "Rendering state":                  "RS",
#     "Playing bitrate (a/v)":            "PBR",
#     "Playing resolution":               "RES",
#     "Playing/Buffering vmaf":           "VMAF",
#     "Buffering bitrate (a/v)":          "BBR",
#     "Buffer size in Bytes (a/v)":       "BB1",
#     "Buffer size in Bytes":             "BB2",
#     "Buffer size in Seconds (a/v)":     "BSe",
#     "Will Rebuffer":                    "WR",
#     "Current CDN (a/v)":                "CDN",
#     "Audio Track":                      "AT",
#     "Video Track":                      "VT",
#     "Timed Text Track":                 "TT",
#     "Framerate":                        "FR",
#     "Current Dropped Frames":           "CDF",
#     "Total Frames":                     "TF",
#     "Total Dropped Frames":             "TDF",
#     "Total Corrupted Frames":           "TCF",
#     "Total Frame Delay":                "TFD",
#     "Main Thread stall/sec":            "MTS",
#     "VideoDiag":                        "VD",
#     "Throughput":                       "Th",
#     "DFR":                              "DFR",
#     "isPlaying":                        "PL?";

class NetflixSessionError(Exception):
  pass

class NetflixSession:
  TS_GRANULARITY = 1000

  def __init__(self, filename = None):
    """
    Initialize empty stats structure
    :return:
    """
    self.startTime = 0
    self.endTime = 0
    self.mid = ""
    self.shortcutTime = 0
    self.version = ""
    self.esn = ""
    self.userAgent = ""
    self.duration = 0
    self.timeStamps = []
    self.position = []
    self.playingBitrate = []
    self.bufferingBitrate = []
    self.rendState = []
    self.videoBufferSize = []
    self.audioBufferSize = []
    self.videoBufferSizeSeconds = []
    self.audioBufferSizeSeconds = []
    self.resolution = []
    self.readyStates = []
    self.throughput = []
    self.bitrateChanges = []
    self.stalls = []
    self.stalls_from_buffer = []
    self.joinTime = 0
    if filename is not None:
      self.filename = filename
      self.processed_succesfully = self.process_netflix_session()
    else:
      raise NetflixSessionError("Unparsable JSON file")

  def process_netflix_session(self, filename=None):
    """
    Function that gets a json file and return it parsed into an
    :param filename: the JSON file to parse
    :return: the parsed
    """
    try:
      if filename is not None:
        self.filename = filename
      with open(self.filename, 'r') as f:
        string = f.read()
        f.close()
      dataset = json.loads(string, object_pairs_hook=OrderedDict)
    except Exception as e:
      try:
        string = string + "\"}]}"
        dataset = json.loads(string, object_pairs_hook=OrderedDict)
      except Exception as e:
        raise NetflixSessionError("Bad JSON file: " + e.message)
    try:
      self.startTime = int(dataset["st"])
      self.mid = dataset["mid"]
      self.shortcutTime = int(dataset["sct"])
      self.endTime = int(dataset["et"])
      self.version = dataset["vals"][0]["V"]
      self.esn = dataset["vals"][0]["ESN"]
      self.userAgent= dataset["vals"][0]["UA"]
      for entry in dataset["vals"]:
        self.timeStamps.append(int(entry["ts"]) - self.startTime)
        self.add_position(entry)
        self.add_buffering_bitrate(entry)
        self.add_playing_bitrate(entry)
        self.add_buffer_size(entry)
        self.add_buffer_size_seconds(entry)
        self.add_resolution(entry)
        self.add_throughput(entry)
        self.add_renderingstate(entry)
        self.add_readyState(entry)
      self.gen_bitrate_changes()
      self.gen_empty_buffers()
      self.gen_join_time()
      return True

    except KeyError as ke:
      raise NetflixSessionError("Parsing error: KeyError: " + str(ke.message))
    except IndexError as ie:
      raise NetflixSessionError("Parsing error: IndexError: " + str(ie.message))
    except Exception as e:
      raise NetflixSessionError("Parsing error: Exception:" + str(e.message))

  def get_timestamps(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.timeStamps

  def get_timestamp_by_time(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.timeStamps):
      return None
    return self.timeStamps[i]


  def get_timestamp_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.timeStamps[i]

  def add_position(self, entry):
    """
    Parses a buffering bitrate entry from the file
    :param entry:
    :return: nothing
    """
    try:
      pos = entry["Pos"]
      pos = pos.strip()
      self.position.append(float(pos))
    except ValueError as verr:
      self.position.append(0)
    except KeyError:
      if len(self.position) > 0:
        self.position.append(self.position[len(self.position) - 1])
      else:
        self.position.append(0)


  def get_positions(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.position

  def get_position_by_time(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.position):
      return None
    return self.position[i]


  def get_position_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.position[i]

  def add_buffering_bitrate(self, entry):
    """
    Parses a buffering bitrate entry from the file
    :param entry:
    :return: nothing
    """
    try:
      bb2 = entry["BBR"]
      bb2 = bb2.strip()
      if bb2 == "?":
        self.bufferingBitrate.append(0)
      else:
        bb2 = bb2.split("/")
        self.bufferingBitrate.append(float(bb2[0].strip()) + float(bb2[1].strip()))

    except KeyError:
      if len(self.bufferingBitrate) > 0:
        self.bufferingBitrate.append(self.bufferingBitrate[len(self.bufferingBitrate) - 1])
      else:
        self.bufferingBitrate.append(0)

  def get_buffering_bitrates(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.bufferingBitrate

  def get_buffering_bitrate_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.bufferingBitrate):
      return None
    return self.bufferingBitrate[i]

  def get_buffering_bitrate_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.bufferingBitrate[i]

  def add_playing_bitrate(self, entry):
    """
    Parses a playing bitrate entry from the file
    :param entry:
    :return: nothing
    """
    try:
      bb1 = entry["PBR"]
      bb1 = bb1.strip()
      if bb1 == "?":
        self.playingBitrate.append(0)
      else:
        bb1 = bb1.split("/")
        self.playingBitrate.append(float(bb1[0].strip()) + float(bb1[1].strip().split("(")[0]))
    except KeyError:
      if len(self.playingBitrate) > 0:
        self.playingBitrate.append(self.playingBitrate[len(self.playingBitrate) - 1])
      else:
        self.playingBitrate.append(0)

  def add_readyState(self, entry):
    try:
      videoDiag = entry["VD"]
      info = videoDiag.split(',')
      value = None
      for el in info:
        if el.startswith('readyState'):
          value = int(el.split('=')[1])
          self.readyStates.append(value)
      if value is None:
        raise ValueError()
    except ValueError as verr:
      self.readyStates.append(0)
    except KeyError:
      if len(self.readyStates) > 0:
        self.readyStates.append(self.readyStates[-1])
      else:
        self.readyStates.append(0)

  def get_playing_bitrates(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.playingBitrate

  def get_playing_bitrate_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.playingBitrate):
      return None
    return self.playingBitrate[i]

  def get_playing_bitrate_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.playingBitrate[i]

  def add_buffer_size(self, entry):
    """
    Parses a buffering bitrate entry from the file
    :param entry:
    :return: nothing
    """
    try:
      bb1 = entry["BB1"]
      bb1 = bb1.strip()
      bb1 = bb1.split("/")
      self.audioBufferSize.append(int(bb1[0].strip()))
      self.videoBufferSize.append(int(bb1[1].strip()))
    except ValueError as verr:
      self.videoBufferSize.append(0)
      self.audioBufferSize.append(0)
    except KeyError:
      if len(self.videoBufferSize) > 0:
        self.videoBufferSize.append(self.videoBufferSize[-1])
        self.audioBufferSize.append(self.audioBufferSize[-1])
      else:
        self.videoBufferSize.append(0)
        self.audioBufferSize.append(0)

  def add_buffer_size_seconds(self, entry):
    """
    Parses a buffering bitrate in seconds entry from the file
    :param entry:
    :return: nothing
    """
    try:
      bb1 = entry["BSe"]
      bb1 = bb1.strip()
      bb1 = bb1.split("/")
      self.audioBufferSizeSeconds.append(float(bb1[0].strip()))
      self.videoBufferSizeSeconds.append(float(bb1[1].strip()))
    except ValueError as verr:
      self.videoBufferSizeSeconds.append(0)
      self.audioBufferSizeSeconds.append(0)
    except KeyError:
      if len(self.videoBufferSizeSeconds) > 0:
        self.videoBufferSizeSeconds.append(self.videoBufferSizeSeconds[-1])
        self.audioBufferSizeSeconds.append(self.audioBufferSizeSeconds[-1])
      else:
        self.videoBufferSizeSeconds.append(0)
        self.audioBufferSizeSeconds.append(0)

  def get_video_buffer_sizes(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.videoBufferSize

  def get_video_buffer_size_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.videoBufferSize):
      return None
    return self.videoBufferSize[i]

  def get_video_buffer_size_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.videoBufferSize[i]

  def get_video_buffer_sizes_seconds(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.videoBufferSizeSeconds

  def get_video_buffer_size_seconds_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.videoBufferSize):
      return None
    return self.videoBufferSizeSeconds[i]

  def get_video_buffer_size_seconds_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.videoBufferSizeSeconds[i]

  def get_audio_buffer_sizes(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.audioBufferSize

  def get_audio_buffer_size_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.audioBufferSize):
      return None
    return self.audioBufferSize[i]

  def get_audio_buffer_size_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.audioBufferSize[i]

  def get_audio_buffer_sizes_seconds(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.audioBufferSizeSeconds

  def get_audio_buffer_size_seconds_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.audioBufferSizeSeconds):
      return None
    return self.audioBufferSizeSeconds[i]

  def get_audio_buffer_size_seconds_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.audioBufferSizeSeconds[i]

  def add_resolution(self, entry):
    """
    Parses a resolution entry from the file
    :param entry:
    :return: nothing
    """
    try:
      if "Res" in entry.keys():
        res = entry["Res"]
        res = res.strip()
        res = res.split("/")
        self.resolution.append([res[0],res[1]])
      elif "PBR" in entry.keys() and "?" not in entry["PBR"]:
        s = entry["PBR"]
        print s
        res = s[s.find("(")+1:s.find(")")].split("x")
        print res
        self.resolution.append([res[0], res[1]])
      else:
        if len(self.resolution) > 0:
          self.resolution.append(self.resolution[len(self.resolution) - 1])
        else:
          self.resolution.append([0, 0])
    except KeyError:
      if len(self.resolution) > 0:
        self.resolution.append(self.resolution[len(self.resolution) - 1])
      else:
        self.resolution.append([0,0])

  def get_resolutions(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.resolution

  def get_resolution_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.resolution):
      return None
    return self.resolution[i]

  def get_resolution_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.resolution[i]

  def add_throughput(self, entry):
    """
    Parses a throughput entry from the file
    :param entry:
    :return: nothing
    """
    try:
      th = entry["Th"]
      self.throughput.append(int(th))
    except (ValueError, TypeError) as verr:
      self.throughput.append(0)
    except KeyError:
      self.throughput.append(self.throughput[-1] if len(self.throughput) > 0 else 0)


  def get_throughput(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.throughput

  def get_throughput_by_time(self, t):
    """

    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.throughput):
      return None
    return self.throughput[i]

  def get_throughput_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.throughput[i]

  def add_renderingstate(self, entry):
    """
    Parses a buffering bitrate entry from the file
    :param entry:
    :return: nothing
    """
    try:
      rs = entry["RS"]
      rs = rs.strip()
      self.rendState.append(rs)
    except KeyError:
      self.rendState.append("")


  def get_renderingstate(self):
    """
    Temp
    :param self:
    :return:
    """
    return self.position

  def get_renderingstate_by_time(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/NetflixSession.TS_GRANULARITY)
    if i >= len(self.position):
      return None
    return self.position[i]


  def get_renderingstate_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.position[i]

  def gen_bitrate_changes(self):
    """
    Goes through the buffering bitrates and tracks eventual changes over time
    :return:
    """
    started = False
    for index in range(len(self.bufferingBitrate)):
      if index == 0:
        continue
      if not started and self.bufferingBitrate[index] > 0:
        started = True
        continue
      if self.bufferingBitrate[index] > self.bufferingBitrate[index-1]:
        self.bitrateChanges.append({"ts": self.get_timestamp_by_index(index),
                                    "direction": "up",
                                    "previous": self.bufferingBitrate[index-1],
                                    "new": self.bufferingBitrate[index]})
      elif self.bufferingBitrate[index] < self.bufferingBitrate[index-1]:
        self.bitrateChanges.append({"ts": self.get_timestamp_by_index(index),
                                    "direction": "down",
                                    "previous": self.bufferingBitrate[index-1],
                                    "new": self.bufferingBitrate[index]})

  def gen_empty_buffers_from_buffer(self):
    """
    Goes through the buffer sizes and returns empty instances. Skips the first entries at 0
    :return:
    """
    started = False
    current_stall = None
    for index in range(len(self.videoBufferSize)):
      if not started and self.videoBufferSize[index] > 0:
        started = True
        continue
      elif self.videoBufferSize[index] != 0:
        if current_stall is not None:
          #Close current stall
          current_stall["end"] = self.get_timestamp_by_index(index) - 1 - \
                                 (self.get_position_by_index(index) - self.get_position_by_index(index - 1))
          self.stalls.append(current_stall)
          current_stall = None
      else:
        if current_stall is None:
          current_stall = {"start": self.get_timestamp_by_index(index) - 1 -
                                    (self.get_position_by_index(index) - self.get_position_by_index(index - 1)),
                           "end": 0}
    if current_stall is not None:
      current_stall["end"] = self.endTime
      self.stalls.append(current_stall)

  def gen_empty_buffers(self):
    """
    Goes through the buffer sizes and returns empty instances. Skips the first entries at 0
    :return:
    """
    started = False
    current_stall = None
    for index in range(len(self.readyStates)):
      if not started and self.readyStates[index] != 2:
        started = True
        continue
      # elif self.videoBufferSize[index] != 0:
      elif self.readyStates[index] != 2:
        if current_stall is not None:
          # Close current stall
          current_stall["end"] = self.get_timestamp_by_index(index)
          self.stalls.append(current_stall)
          current_stall = None
      elif self.readyStates[index] == 2:
        if current_stall is None:
          current_stall = {"start": self.get_timestamp_by_index(index),
                           "end": 0}
    if current_stall is not None:
      current_stall["end"] = self.endTime
      self.stalls.append(current_stall)

  def gen_join_time(self):
    '''

    :return:
    '''
    for index in range(len(self.rendState)):
      if self.rendState[index] == "Playing":
        self.joinTime = self.timeStamps[index] - (self.position[index] - self.position[index-1])
        break


  def get_bitrate_changes(self):
    """
    Goes through the buffering events and builds start and end time windows
    :return:
    """
    return self.bitrateChanges

  def get_empty_buffers(self):
    """
    Goes through the buffer sizes and returns empty instances
    :return:
    """
    return self.stalls

  def get_empty_buffers_from_buffer(self):
    """
    Goes through the buffer sizes and returns empty instances
    :return:
    """
    return self.stalls_from_buffer

  def get_join_time(self):
    '''

    :return:
    '''
    return self.joinTime