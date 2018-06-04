import argparse
import os
import datetime
from pytz import timezone
import re
import plot
from plot import plt
import sys
from time import mktime
import numpy as np
from netflix_session import NetflixSession
from youtube_session import YoutubeSession
from plot_session import plot_netflix_session,plot_youtube_session


def mean(numbers):
  return float(sum(numbers)) / max(len(numbers), 1)

def parse_n_extension_squantiles(extfolder, recursive=True):
  """
  Parses files in a folder and extracts throughput values over time for a the entire system or for a specific device
  :param folder: the folder to parse
  :param device: the device to parse throughput for. If none, it parses the throughput for the entire system
  :param day: specifies the date for which to get throughput values. If none, parses all files in a folder
  :return: an ordered list of timestamps and througput values
  """
  sessions = []
  dirs = os.listdir(extfolder)
  # print "Parsing folder ", extfolder
  for dir in dirs:
    if os.path.isdir(extfolder+dir):
      # print "Parsing folder ", dir
      fs = os.listdir(extfolder+dir)
      for f in fs:
        # print "Parsing file ", f
        if f.endswith("json"):
          fn = f.split('/')[-1]
          sessiontype = fn.split('_')[2]
          ip = fn.split('_')[1]
          if sessiontype == 'n' and "1935224" in ip:
            try:
              nf = NetflixSession(filename=extfolder+dir+"/"+f)
              positions = nf.get_positions()
              for i in range(len(positions)):
                if positions[i] > 0:
                  sessions.append(nf.get_timestamp_by_index(i))
                  break
            except KeyError:
              continue
            except Exception as e:
              # print "Not possible to parse", extfolder+dir+"/"+f,"because", st, et, timearrs, trafarrs, e
              print "Not possible to parse netflix", extfolder + dir + "/" + f, "because", e
              # sys.exit(0)
    else:
      print "Not a dir ", extfolder+dir
  #compute quantiles
  a = np.array(sessions)
  print "AVG:", mean(sessions)
  print "FINAL RESULT:", np.percentile(a, 25), np.percentile(a, 50), np.percentile(a, 75), np.percentile(a, 100)

def parse_n_extension_traffic(extfolder, recursive=True):
  """
  Parses files in a folder and extracts throughput values over time for a the entire system or for a specific device
  :param folder: the folder to parse
  :param device: the device to parse throughput for. If none, it parses the throughput for the entire system
  :param day: specifies the date for which to get throughput values. If none, parses all files in a folder
  :return: an ordered list of timestamps and througput values
  """
  sessions = []
  dirs = os.listdir(extfolder)
  # print "Parsing folder ", extfolder
  for dir in dirs:
    if os.path.isdir(extfolder+dir):
      # print "Parsing folder ", dir
      fs = os.listdir(extfolder+dir)
      for f in fs:
        # print "Parsing file ", f
        if f.endswith("json"):
          fn = f.split('/')[-1]
          sessiontype = fn.split('_')[2]
          ip = fn.split('_')[1]
          if sessiontype == 'n' and "1935224" in ip:
            try:
              nf = NetflixSession(filename=extfolder+dir+"/"+f)
              # print extfolder + dir + "/" + f, " processed succesfully"
              sessions.append(nf)
            except KeyError:
              continue
            except Exception as e:
              # print "Not possible to parse", extfolder+dir+"/"+f,"because", st, et, timearrs, trafarrs, e
              print "Not possible to parse netflix", extfolder + dir + "/" + f, "because", e
              # sys.exit(0)
    else:
      print "Not a dir ", extfolder+dir
  return sessions


def parse_y_extension_traffic(extfolder, recursive=True):
  """

  """
  dirs = os.listdir(extfolder)
  # print "Parsing folder ", extfolder
  for dir in dirs:
    if os.path.isdir(extfolder+dir):
      # print "Parsing folder ", dir
      fs = os.listdir(extfolder+dir)
      for f in fs:
        # print "Parsing file ", f
        if f.endswith("json"):
          fn = f.split('/')[-1]
          sessiontype = fn.split('_')[2]
          ip = fn.split('_')[1]
          if sessiontype == 'y' and "1935224" in ip:
            try:
              with open(extfolder+dir+"/"+f, 'r') as jsonstr:
                yt = YoutubeSession(filename=jsonstr)
                jsonstr.close()
            except Exception as e:
              # print "Not possible to parse", extfolder+dir+"/"+f,"because", st, et, timearrs, trafarrs, e
              print "Not possible to parse youtube", extfolder + dir + "/" + f, "because", e
    else:
      print "Not a dir ", extfolder+dir


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-f', '--folder', type=str, required=True, help="folder to process")
  parser.add_argument('-n', '--netflix', action='store_true', help="parse netflix")
  parser.add_argument('-y', '--youtube', action='store_true', help="parse youtube")
  parser.add_argument('-p', '--plot', action='store_true', help="plot processed")
  parser.add_argument('-r', '--recursive', action='store_true', help="recursive in folders")

  args = vars(parser.parse_args())

  parse_n_extension_squantiles(args['folder'])
  return

  nsessions = []
  ysessions = []
  if args['netflix']:
    nsessions = parse_n_extension_traffic(args['folder'], recursive=args['recursive'])

  if args['youtube']:
    ysessions = parse_y_extension_traffic(args['folder'], recursive=args['recursive'])

  if args['plot']:
    #create plots folder if needed
    for session in nsessions:
      plot_netflix_session(session, "plots/")

    for session in ysessions:
      plot_youtube_session(session, "plots/")


if __name__ == '__main__':
  main()