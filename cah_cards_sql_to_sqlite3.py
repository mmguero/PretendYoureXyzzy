#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sqlite3
import re
import sys

import mmguero
from mmguero import eprint

###################################################################################################
args = None
debug = False
script_name = os.path.basename(__file__)
script_path = os.path.dirname(os.path.realpath(__file__))
orig_path = os.getcwd()

###################################################################################################
# main
def main():
  global args
  global debug

  parser = argparse.ArgumentParser(description=script_name, add_help=False, usage='{} <arguments>'.format(script_name))
  parser.add_argument('-d', '--defaults', dest='accept_defaults', type=mmguero.str2bool, nargs='?', const=True, default=False, metavar='true|false', help="Accept defaults to prompts without user interaction")
  parser.add_argument('-v', '--verbose', dest='debug', type=mmguero.str2bool, nargs='?', const=True, default=False, metavar='true|false', help="Verbose/debug output")
  parser.add_argument('-i', '--input', dest='input', type=str, default='cah_cards.sql', required=False, metavar='<string>', help="Input")
  parser.add_argument('-o', '--output', dest='output', type=str, default='new.sqlite3', required=False, metavar='<string>', help="Output")
  try:
    parser.error = parser.exit
    args = parser.parse_args()
  except SystemExit:
    parser.print_help()
    exit(2)

  debug = args.debug
  if debug:
    eprint(os.path.join(script_path, script_name))
    eprint("Arguments: {}".format(sys.argv[1:]))
    eprint("Arguments: {}".format(args))
  else:
    sys.tracebacklimit = 0

  # load up input SQL statements, put everything in statements
  # except for CREATE TABLE, which we put in createTables
  inLines = None
  with open(args.input, 'r') as f:
    inLines = [x.strip() for x in f.readlines() if ((not x.strip().startswith('--')) and
                                                    (not x.strip().startswith('SET')) and
                                                    (not x.strip().startswith('COMMENT')) and
                                                    (not x.strip() == ""))]

  createTables = {}
  statements = []
  currentStatement = None
  endOfStatement = ';'
  createRegex = re.compile(r'^\s*CREATE\s*TABLE\s*(\S+)')
  for line in inLines:
    if currentStatement is None:
      currentStatement = []
    currentStatement.append(line)
    if line.endswith(endOfStatement):
      if line.startswith('COPY') and line.endswith("stdin;"):
        endOfStatement = '\\.'
      else:
        firstLine = line if (len(currentStatement) == 0) else currentStatement[0]
        if match := createRegex.match(firstLine):
          createTables[match[1]] = currentStatement
        else:
          statements.append(currentStatement)
        currentStatement = None
        endOfStatement = ';'

  db = None
  try:
    if os.path.isfile(args.output):
      os.unlink(args.output)

    db = sqlite3.connect(args.output)
    cursor = db.cursor()

    # add primary/foreign key constraints to create statements
    pkeyRegex = re.compile(r'ALTER\s+TABLE\s+(ONLY\s+)?(\S+)\s+ADD\s+(CONSTRAINT\s+(\S+)\s+(PRIMARY)\s+KEY\s+\((.+)\)(\s+REFERENCES\s+.+)?)')
    for statement in statements:
      if pkeyMatch := pkeyRegex.search(" ".join(statement)):
        createStatement = createTables[pkeyMatch.group(2)]
        if createStatement:
          createStatement[-2] = f"{createStatement[-2]},"
          createStatement.insert(-1, f"primary key ({pkeyMatch.group(6)})")

    # create tables
    for table, statement in createTables.items():
      fullStatement = re.sub(r'character\s+varying\s*\(\d+\)', 'varchar', " ".join(statement), flags=re.IGNORECASE)
      if debug:
        eprint(fullStatement)
      try:
        cursor.execute(fullStatement)
      except sqlite3.Error as err:
        eprint(err)
    db.commit()

    # exeute other statements (?)
    for statement in statements:
      if statement[0].startswith('CREATE EXTENSION'):
        pass
      elif statement[0].startswith('CREATE SEQUENCE'):
        pass
      elif statement[0].startswith('ALTER') and (any('OWNER TO' in x for x in statement) or any('ADD CONSTRAINT' in x for x in statement)):
        pass
      elif statement[0].startswith('COPY') and any('stdin' in x for x in statement):
        pass
      elif any('pg_catalog.setval' in x for x in statement):
        pass
      else:
        if debug:
          eprint(" ".join(statement))
        try:
          cursor.execute(" ".join(statement))
        except sqlite3.Error as err:
          eprint(err)
    db.commit()

    # insert data
    copyRegex = re.compile(r'COPY\s+(\S+)\s*\((.+)\)\s*FROM')
    for statement in statements:
      insertStatement = None
      firstValuesLine = 0
      if statement[0].startswith('COPY') and any('stdin' in x for x in statement):
        copyStatement = []
        for line in statement:
          copyStatement.append(line)
          firstValuesLine = firstValuesLine + 1
          if line.endswith(';'):
            break
        if copyMatch := copyRegex.search(" ".join(copyStatement)):
          insertStatement = f"INSERT INTO {copyMatch.group(1)} ({copyMatch.group(2)}) VALUES ({', '.join(['?' for _ in range(len([x.strip() for x in copyMatch.group(2).split(',')]))])})"
          if debug:
            eprint(insertStatement)
          for record in statement[firstValuesLine:-1]:
            try:
              cursor.execute(insertStatement, record.split('\t'))
            except sqlite3.Error as err:
              eprint(err)
          db.commit()


  except Exception as e:
    eprint(e)

  finally:
    if db:
      db.close()


###################################################################################################
if __name__ == '__main__':
  main()
