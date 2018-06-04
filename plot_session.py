import argparse
import plot
import os
import datetime
from netflix_session import NetflixSession
from youtube_session import YoutubeSession


def plot_scatter_timeline(x, y, ylabel="",fname="plots/out.png"):
  plot.figsize(8, 3)
  xmax = max(x)
  xmin = min(x)
  xmin = min(xmin-xmin*.1, 0)
  ymax = max(y)
  ymax = ymax+ymax*.1
  ymin = 0
  plot.plot_ts(xarr=x, yarr=y, cnt=0, ptype="scatter", color=True, markersize=5)
  plot.figstuff(ylabel=ylabel, xlabel='Time (s)', ylim=[ymin, ymax], xlim=[xmin, xmax])
  plot.legend(fn=fname)

def plot_timeline(x, y, ylabel="",fname="plots/out.png"):
  plot.figsize(8, 3)
  xticks = [[], []]
  xmax = max(x)
  xmin = min(x)
  ymax = max(y)
  ymax = ymax+ymax*.1
  ymin = 0
  slot = (int(x[-1]) - int(x[0])) / 10
  if slot > 0:
    for i in range(int(x[0]), int(x[-1]), slot):
      xticks[0].append(i)
      ttz = datetime.datetime.fromtimestamp(i/1000)
      xticks[1].append(ttz.strftime('%H:%M'))
  else:
    xticks[0].append(x[0])
    ttz = datetime.datetime.fromtimestamp(x[0] / 1000)
    xticks[1].append(ttz.strftime('%H:%M'))
    xticks[0].append(x[-1])
    ttz = datetime.datetime.fromtimestamp(x[-1] / 1000)
    xticks[1].append(ttz.strftime('%H:%M'))
  plot.plot_ts(xarr=x, yarr=y, cnt=0, ptype="plot", color=True, markersize=5)
  plot.figstuff(xticks=xticks, ylabel=ylabel, xlabel='Time (s)', ylim=[ymin, ymax], xlim=[xmin, xmax])
  plot.legend(fn=fname)

def plot_netflix_buffering_bitrate(netflixSession):
  x = netflixSession.get_timestamps()
  x[:] = [a/1000 for a in x]
  y = netflixSession.get_buffering_bitrates()
  plot_timeline(x,y,ylabel='buffering bitrate',fname='plots/netflix_buffering_bitrate.png')

def plot_youtube_buffersize(youtubeSession):
  x = youtubeSession.get_timestamps()
  # x[:] = [a/1000 for a in x]
  y = youtubeSession.get_buffer_durations()
  plot_timeline(x,y,ylabel='buffering bitrate',fname='plots/youtube_buffersize.png')

def plot_netflix_session(netflixSession, dstfolder=""):
  # create session name folder
  filename = netflixSession.filename.split("/")[-1]
  if not os.path.exists(dstfolder+filename):
    os.makedirs(dstfolder+filename)
  x = netflixSession.get_timestamps()
  x = [ts + netflixSession.startTime for ts in x]
  y = netflixSession.get_buffering_bitrates()
  plot_timeline(x,y,ylabel='buffering bitrate (kbps)',fname=dstfolder+filename+'/buffering_bitrate.png')
  y = netflixSession.get_video_buffer_sizes()
  plot_timeline(x,y,ylabel='buffer size (B)',fname=dstfolder+filename+'/buffer_size.png')

def plot_youtube_session(youtubeSession, dstfolder=""):
  x = youtubeSession.get_timestamps()
  y = youtubeSession.get_buffer_durations()
  # create session name folder

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-f', '--filename', type=file, required=True, help="file")
  parser.add_argument('-s', '--service', type=str, required=True, help="service n|y")
  parser.add_argument('-p', '--plots', type=str, required=True, help="list of plots "
                                                                     "separated by commas [bbr|pbr]")
  args = vars(parser.parse_args())
  if args['service'] == 'n':
    session = NetflixSession(filename=args["filename"])
    plot_netflix_buffering_bitrate(session)
  elif args['service'] == 'y':
    session = YoutubeSession(filename=args["filename"])
    plot_youtube_buffersize(session)


if __name__ == '__main__':
  main()