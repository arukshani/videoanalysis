import json
from collections import OrderedDict

# YPR: available video rates ["hd2160","hd1440","hd1080","hd720","large","medium","small","tiny","auto"]
# BUF: rangesToString(video.buffered),
# CRS: video.currentSrc,
# CUT: video.currentTime,
# DFR: video.defaultPlaybackRate,
# DUR: video.duration,
# END: video.ended,
# LOO: video.loop,
# NST: video.networkState,
# PAU: video.paused,
# PRA: video.playbackRate,
# PLA: rangesToString(video.played),
# RST: video.readyState,
# SEB: rangesToString(video.seekable),
# SEI: video.seeking,
# SRC: video.src,
# SDA: video.startDate,
# VTR: video.videoTracks,
# VOL: video.volume,
# VWI: video.videoWidth,
# VHE: video.videoHeight,
# WFC: video.webkitDecodedFrameCount,
# WDR: video.webkitDroppedFrameCount,
# WAD: video.webkitAudioDecodedByteCount,
# WVD: video.webkitVideoDecodedByteCount
# EVE: video event -> cf ../netflix_stats_extension/youtube/youtube_cs.js for integer mappings

# var videoEvents = {
#     "abort": 1, // abort	Fires when the loading of an audio/video is aborted
#     "canplay": 2, // canplay	Fires when the browser can start playing the audio/video
#     "canplaythrough": 3, // canplaythrough	Fires when the browser can play through the audio/video without stopping for buffering
#     "durationchange": 4, //     durationchange	Fires when the duration of the audio/video is changed
#     "emptied": 5, // emptied	Fires when the current playlist is empty
#     "ended": 6, // ended	Fires when the current playlist is ended
#     "error": 7, // error	Fires when an error occurred during the loading of an audio/video
#     "loadeddata": 8, // loadeddata	Fires when the browser has loaded the current frame of the audio/video
#     "loadedmetadata": 9, // loadedmetadata	Fires when the browser has loaded meta data for the audio/video
#     "loadstart": 10, // loadstart	Fires when the browser starts looking for the audio/video
#     "pause": 11, // pause	Fires when the audio/video has been paused
#     "play": 12, // play	Fires when the audio/video has been started or is no longer paused
#     "playing": 13, // playing	Fires when the audio/video is playing after having been paused or stopped for buffering
#     "progress": 14, //     progress	Fires when the browser is downloading the audio/video
#     "ratechange": 15, // ratechange	Fires when the playing speed of the audio/video is changed
#     "seeked": 16, // seeked	Fires when the user is finished moving/skipping to a new position in the audio/video
#     "seeking": 17, // seeking	Fires when the user starts moving/skipping to a new position in the audio/video
#     "stalled": 18, // stalled	Fires when the browser is trying to get media data, but data is not available
#     "suspend": 19, // suspend	Fires when the browser is intentionally not getting media data
#     "timeupdate": 20, // timeupdate	Fires when the current playback position has changed
#     "volumechange": 21, // volumechange	Fires when the volume has been changed
#     "waiting": 22 // waiting	Fires when the video stops because it needs to buffer the next frame
# };

class GeneralSessionError(Exception):
  pass

class GeneralSession:

  TS_GRANULARITY = 500

  def __init__(self, filename = None):
    """
    Initialize empty stats structure
    :return:
    """
    self.mid = ""
    self.startTime = 0
    self.endTime = 0
    self.durations = []
    self.timeStamps = []
    self.readyStates = []
    self.buffer = []
    self.bufferDurations = []
    self.currentTimes = []
    self.position = []
    self.videoWidths = []
    self.videoHeights = []
    self.videoRates = []
    self.resolution = []
    self.webkitAudioDecodedByteCount = []
    self.webkitVideoDecodedByteCount = []
    self.events = []
    self.eventsTimeStamps = []
    self.bitrateChanges = []
    self.stalls = []
    self.joinTime = 0
    self.version = None
    self.isAborted = False
    if filename is not None:
      self.processed_succesfully = self.process_youtube_session(filename)
    else:
      raise GeneralSessionError("Unparsable JSON file")


  def process_youtube_session(self, filename):
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
      dataset = json.loads(string)
    except Exception as e:
      raise GeneralSessionError("Bad JSON file: " + e.message)
    try:
      self.startTime = float(dataset["st"])
      self.endTime = float(dataset["et"])
      if "v" in dataset:
        self.version = dataset["v"]
      self.mid = dataset["mid"]

      for entry in dataset["vals"]:
        ts = int(entry["ts"]) - self.startTime
        if not self.add_event(entry, ts):
          self.timeStamps.append(ts)
          self.add_buffer(entry)
          self.add_video_height(entry)
          self.add_video_width(entry)
          self.add_currentTime_video(entry)
          self.add_ready_state(entry)
          self.add_decoded_video_bytes(entry)
          self.add_duration(entry)
      self.add_video_rates()
      self.add_buffer_durations()
      self.throw_last_video()
      self.gen_join_time()
      self.gen_bitrate_changes()
      self.gen_empty_buffers()
      return True

    except KeyError as ke:
      raise GeneralSessionError("Bad JSON file: " + ke.message)

  def add_video_height(self, entry):
    try:
      h = int(entry["VHE"])
      self.videoHeights.append((entry['ts'], h))
    except KeyError:
      if len(self.videoHeights) > 0:
        self.videoHeights.append((entry['ts'], self.videoHeights[-1][1]))
      else:
        self.videoHeights.append((entry['ts'], 0))
    except ValueError as e:
      if len(self.videoHeights) > 0:
        self.videoHeights.append((entry['ts'], self.videoHeights[-1][1]))
      else:
        self.videoHeights.append((entry['ts'], 0))


  def get_video_heights(self):
    return self.videoHeights


  def add_video_width(self, entry):
    try:
      w = int(entry["VWI"])
      self.videoWidths.append((entry['ts'], w))
    except KeyError:
      if len(self.videoWidths) > 0:
        self.videoWidths.append((entry['ts'], self.videoWidths[-1][1]))
      else:
        self.videoWidths.append((entry['ts'], 0))
    except ValueError as e:
      print 'Error while parsing video width: ' + e.message
      if len(self.videoWidths) > 0:
        self.videoWidths.append((entry['ts'], self.videoWidths[-1][1]))
      else:
        self.videoWidths.append((entry['ts'], 0))


  def get_video_widths(self):
    return self.videoWidths


  def add_video_rates(self):
    # from https://support.google.com/youtube/answer/1722171?hl=en (SDR uploads)
    widths = self.get_video_widths()
    heights = self.get_video_heights()

    for i in range(len(heights)):
      if heights[i] == 2160:
        self.videoRates.append(40)
      elif heights[i] == 1440:
        self.videoRates.append(16)
      elif heights[i] == 1080:
        self.videoRates.append(8)
      elif heights[i] == 720:
        self.videoRates.append(5)
      elif heights[i] == 480:
        self.videoRates.append(2.5)
      elif heights[i] == 360:
        self.videoRates.append(1)
      elif heights[i] == 240:
        self.videoRates.append(0.5)
      elif heights[i] == 144:
        self.videoRates.append(0.25)
      else:
        self.videoRates.append(0)


  def add_currentTime_video(self, entry):
    try:
      ct = float(entry["CUT"])
      self.currentTimes.append(ct)
    except KeyError:
      if len(self.currentTimes) == 0:
        self.currentTimes.append(0)
      else:
        self.currentTimes.append(self.currentTimes[-1])
    except ValueError as e:
      if len(self.currentTimes) == 0:
        self.currentTimes.append(0)
      else:
        self.currentTimes.append(self.currentTimes[-1])


  def get_currentTimes_video(self):
    return self.currentTimes

  def get_currentTimes_by_time(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/500)
    if i >= len(self.currentTimes):
      return None
    return self.currentTimes[i]


  def get_currentTimes_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.currentTimes[i]


  def get_video_rates(self):
    return self.videoRates


  def add_duration(self, entry):
    try:
      h = int(entry["DUR"])
      self.durations.append(h)
    except TypeError:
      self.durations.append(0)
      return None
    except KeyError:
      self.durations.append(0)
      return None


  def get_durations(self):
    return self.durations

  def get_duration_by_time(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/500)
    if i >= len(self.currentTimes):
      return None
    return self.durations[i]


  def get_duration_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.durations[i]


  def add_buffer(self, entry):
    try:
      bufferE = entry["BUF"]
      if not bufferE:
        raise KeyError()

      if self.version is not None:
        buffer = bufferE[2:-2]
      else:
        buffer = bufferE[2:-1]
      parts = buffer.split('},{')

      buffer = []
      for part in parts:
        bounds = part.split(',')
        beginning = float(bounds[0][2:])
        end = float(bounds[1][2:])
        buffer.append((beginning, end))
      self.buffer.append(buffer)
    except KeyError as e:
      if len(self.buffer) == 0:
        self.buffer.append([(0, 0)])
      else:
        self.buffer.append(self.buffer[-1])
    except ValueError as e:
      if len(self.buffer) == 0:
        self.buffer.append([(0, 0)])
      else:
        self.buffer.append(self.buffer[-1])


  def get_buffer(self):
    return self.buffer

  def get_buffer_by_time(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime)/500)
    if i >= len(self.buffer):
      return None
    return self.buffer[i]


  def get_buffer_by_index(self, i):
    """

    :param self:
    :param i:
    :return:
    """
    return self.buffer[i]


  def add_buffer_durations(self):
    for i in range(len(self.buffer)):
      dur = None
      for bufferPart in self.buffer[i]:
        start = bufferPart[0]
        end = bufferPart[1]
        if start <= self.currentTimes[i] <= end:
          dur = end - self.currentTimes[i]
          self.bufferDurations.append(dur)
          break
      if dur is None:
        maxEnd = max(self.buffer[i], key=lambda tup: tup[1])[1]
        self.bufferDurations.append(maxEnd - self.currentTimes[i])

      #self.bufferDurations.append(dur if dur >= 0 else 0)


  def get_buffer_durations(self):
    return self.bufferDurations


  def check_if_buffering(self):
    bufferEvents = []
    for i in range(len(self.buffer)):
      if self.bufferDurations[i] == 0 or not (self.buffer[i][0] <= self.currentTimes[i] <= self.buffer[i][1]):
        bufferEvents.append(i)
    return bufferEvents


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
    i = int((t-self.startTime)/500)
    if i >= len(self.timeStamps):
      return None
    return self.timeStamps[i]

  def get_timestamp_by_time_s(self, t):
    """
    Return the closest entry to timestamp t
    :param self:
    :param t:
    :return:
    """
    if t<=0 or t<=self.startTime:
      return None
    i = int((t-self.startTime*1000)/GeneralSession.TS_GRANULARITY)
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

  def add_resolution(self, entry):
    """
    Parses a buffering bitrate entry from the file
    :param entry:
    :return: nothing
    """
    try:
      res = entry["Res"]
      res = res.strip()
      res = res.split("/")
      self.resolution.append([res[0],res[1]])
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
    i = int((t-self.startTime)/GeneralSession.TS_GRANULARITY)
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

  def add_event(self, entry, ts):
    try:
      event = int(entry["EVE"])
      self.events.append(event)
      self.eventsTimeStamps.append(ts)
      return True
    except KeyError:
      return False
    except ValueError as e:
      print 'Error when parsing an event: ' + e.message
      return True


  def add_ready_state(self, entry):
    try:
      rst = int(entry["RST"])
      self.readyStates.append(rst)
    except KeyError:
      if len(self.readyStates) == 0:
        self.readyStates.append(0)
      else:
        self.readyStates.append(self.readyStates[-1])
    except ValueError as e:
      if len(self.readyStates) == 0:
        self.readyStates.append(0)
      else:
        self.readyStates.append(self.readyStates[-1])

  def get_ready_states(self):
    return self.readyStates

  def add_decoded_video_bytes(self, entry):
    try:
      decVidBytes = int(entry["WVD"])
      self.webkitVideoDecodedByteCount.append(decVidBytes)
    except KeyError:
      if (len(self.webkitVideoDecodedByteCount) == 0):
        self.webkitVideoDecodedByteCount.append(0)
      else:
        self.webkitVideoDecodedByteCount.append(self.webkitVideoDecodedByteCount[-1])
    except ValueError as e:
      if (len(self.webkitVideoDecodedByteCount) == 0):
        self.webkitVideoDecodedByteCount.append(0)
      else:
        self.webkitVideoDecodedByteCount.append(self.webkitVideoDecodedByteCount[-1])

  def get_decoded_video_bytes(self):
    return self.webkitVideoDecodedByteCount

  def get_abort_event_index(self):
    try:
      abortIdx = self.events.index(1)
      if self.timeStamps[abortIdx] - self.timeStamps[0] <= 20 and abortIdx < len(self.events) - 1:
        return abortIdx
      else:
        return -1
    except:
      return -1

  def get_buffer_change_index(self, start):
    for i in range(start, len(self.buffer) - 1):
      if self.buffer[i] != self.buffer[i + 1]:
        return i + 1
    return -1

  def throw_last_video(self):
    abortIdx = self.get_abort_event_index()
    if abortIdx >= 0:
      bCIdx = self.get_buffer_change_index(abortIdx + 1)
      #TODO make sure that you do it proportionally to time
      if bCIdx >= 0:
        self.timeStamps = self.timeStamps[bCIdx:]
        self.readyStates = self.readyStates[bCIdx:]
        self.buffer = self.buffer[bCIdx:]
        self.bufferDurations = self.bufferDurations[bCIdx:]
        self.currentTimes = self.currentTimes[bCIdx:]
        self.position = self.position[bCIdx:]
        self.videoWidths = self.videoWidths[bCIdx:]
        self.videoHeights = self.videoHeights[bCIdx:]
        self.videoRates = self.videoRates[bCIdx:]
        self.resolution = self.resolution[bCIdx:]
        self.webkitAudioDecodedByteCount = self.webkitAudioDecodedByteCount[bCIdx:]
        self.webkitVideoDecodedByteCount = self.webkitVideoDecodedByteCount[bCIdx:]
        self.events = self.events[bCIdx:]


  def gen_bitrate_changes(self):
    """
    Goes through the buffering events and builds start and end time windows
    :return:
    """
    started = False
    for index in range(len(self.videoHeights)):
      if index == 0:
        continue
      if not started and self.timeStamps[index] > self.joinTime:
        started = True
        continue
      if self.videoHeights[index] > self.videoHeights[index - 1]:
        self.bitrateChanges.append({"ts": self.get_timestamp_by_index(index),
                                    "direction": "up",
                                    "previous": self.videoHeights[index - 1],
                                    "new": self.videoHeights[index]})
      elif self.videoHeights[index] < self.videoHeights[index - 1]:
        self.bitrateChanges.append({"ts": self.get_timestamp_by_index(index),
                                    "direction": "down",
                                    "previous": self.videoHeights[index - 1],
                                    "new": self.videoHeights[index]})

  def gen_empty_buffers(self):
    """
    Goes through the buffer sizes and returns empty instances
    :return:
    """
    started = False
    current_stall = None
    for index in range(len(self.events)):
      if not started and self.events[index] == 12:
        started = True
        continue
      elif self.events[index] == 13:
        if current_stall is not None:
          # Close current stall
          current_stall["end"] = self.eventsTimeStamps[index]
          self.stalls.append(current_stall)
          current_stall = None
      elif self.events[index] == 22:
        if current_stall is None:
          current_stall = {"start": self.eventsTimeStamps[index],
                           "end": 0}
    if current_stall is not None:
      current_stall["end"] = self.endTime
      self.stalls.append(current_stall)

  def gen_join_time(self):
    '''

    :return:
    '''
    for index in range(len(self.events)):
      if self.events[index] == 13:
        self.joinTime = self.eventsTimeStamps[index]
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

  def get_join_time(self):
    '''

    :return:
    '''
    return self.joinTime

  def get_closest_bitrate(self, ts):
    if ts < self.videoHeights[0][0]:
      return None
    else:
      for i in range(len(self.videoHeights)):
        if ts >= self.videoHeights[i][0]:
          if i == len(self.videoHeights) - 1 or ts < self.videoHeights[i + 1][0]:
            return self.videoHeights[i][1]