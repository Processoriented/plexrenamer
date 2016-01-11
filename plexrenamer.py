import sqlite3
import string
import os
import argparse
import shutil

def readSectionLocationsBySectionId(section_id):
	locations = []
	for row in plexdb.execute('SELECT root_path FROM section_locations WHERE library_section_id=?', (section_id,)):
		locations.append(row[0])
	return locations

def readSectionLocation(id):
	for row in plexdb.execute('SELECT root_path FROM section_locations WHERE id=?', (id,)):
		return row[0]

def readSections():
	sections = []
	for row in plexdb.execute("SELECT id, name, section_type FROM library_sections"):
		sections.append({
			"id": row[0],
			"name": row[1],
			"locations": readSectionLocationsBySectionId(row[0])
		})

	return sections

def getMetadataItem(id):
	return plexdb.execute('SELECT metadata_type, title, [index], parent_id FROM metadata_items WHERE id=?', (id,)).fetchone()

def checkSubtitle(original_filename, new_filename):
	subtitle_original_filename = "%s.srt" % os.path.splitext(original_filename)[0]
	if os.path.exists(subtitle_original_filename):
		subtitle_new_filename = "%s.srt" % os.path.splitext(new_filename)[0]
		return {
			"media_parts_id": -1,	# not in the database?
			"original_filename": subtitle_original_filename,
			"new_filename": subtitle_new_filename
		}
	return None

def sanitizeFileName(filename):
	valid_chars = "-_.:,'() %s%s" % (string.ascii_letters, string.digits)
	return ''.join(c for c in filename.strip() if c in valid_chars)

def sanitizeFileNameNsp(filename):
	valid_chars = "-_.:,'() %s%s" % (string.ascii_letters, string.digits)
	rtn = ''.join(c for c in filename.strip() if c in valid_chars)
	return rtn.replace(' ','_')

def isFolderUsed(folder, folders_list):
	for tmp in folders_list:
		if tmp == folder:
			return True

		if tmp[:len("%s/" % folder)] == "%s/" % folder:
			return True
	return False

def isFolderUsedAlternative(folder, folders_list):
	for tmp in folders_list:
		if tmp == folder:
			return True

		if folder[:len("%s/" % tmp)] == "%s/" % tmp:
			return True
	return False

def isFileUsed(filename, folders_list):
	for tmp in folders_list:
		if filename[:len("%s/" % tmp)] == "%s/" % tmp:
			return True
	return False

def guessSectionCleanup(section_id):
	rm_table = []
	blacklist = ['.AppleDouble', 'Network Trash Folder', 'Temporary Items', '.AppleDesktop', '.AppleDB', '_gsdata_']
	locations = readSectionLocationsBySectionId(section_id)
	all_folders = []
	used_folders = []
	all_files = []
	for location in locations:
		for root, dirs, files in os.walk(location):
			skip = False
			for entry in blacklist:
				offset = len(entry) + 1
				if root[-offset:] == "/%s" % entry:
					skip = True
					break
			if skip:
				continue
			
			if root not in all_folders and root != location:
				all_folders.append(root)
				
			for file in files:
				filename = "%s/%s" % (root, file)
				exist = False
				if os.path.splitext(filename)[1] == ".srt":
					for video_ext in [".mkv", ".mov", ".wmv", ".avi", ".m4v", ".mp4"]:
						tmp_filename = "%s%s" % (os.path.splitext(filename)[0], video_ext)
						if plexdb.execute("SELECT COUNT(*) FROM media_parts WHERE file=?", (tmp_filename,)).fetchone()[0] > 0:
							exist = True
				elif plexdb.execute("SELECT COUNT(*) FROM media_parts WHERE file=?", (filename,)).fetchone()[0] > 0:
					exist = True
					
				if not exist:
					all_files.append(filename)
				elif root not in used_folders and root != location:
					used_folders.append(root)

		all_folders.sort(lambda x,y: cmp(len(x), len(y)))
		all_folders_cleand = []
		for folder in all_folders:
			if not isFolderUsed(folder, used_folders):
				if not isFolderUsedAlternative(folder, all_folders_cleand):
					all_folders_cleand.append(folder)
					rm_table.append({
						"type": "folder",
						"path": folder
					})
					
		for filename in all_files:
			if not isFileUsed(filename, all_folders_cleand):
				rm_table.append({
					"type": "file",
					"filename": filename
				})
			
	return rm_table

def dupeCount(metadata_item_id):
	q = 'SELECT count(*) FROM media_items WHERE media_items.metadata_item_id =%d' % metadata_item_id
	try:
		rawCount = plexdb.execute(q)
	except Exception:
		rawCount = 1

	return rawCount - 1

def biggestDupe(metadata_item_id):
	q = 'SELECT media_parts.id FROM media_parts JOIN media_items ON media_parts.media_item_id = media_items.id WHERE '
	
def guessSectionActions(section_id):
	renamed_files_list = []
	rename_table = []
	for row in plexdb.execute("SELECT media_items.metadata_item_id, media_parts.file, media_parts.id, media_items.section_location_id FROM media_parts JOIN media_items ON media_parts.media_item_id = media_items.id WHERE media_items.library_section_id=? ORDER BY media_parts.file", (section_id,)):
		# if dupeCount(row[0]) !== 1:
		# 	testVar = True

		row2 = plexdb.execute('SELECT metadata_type, title, originally_available_at as "[timestamp]", parent_id, [index] FROM metadata_items WHERE id=?', (row[0],)).fetchone()
		try:
			if len(row2[1].strip()) == 0:
				continue
		except TypeError:
			continue
			
		if not os.path.exists(row[1]):
			print "WARNING: file '%s' missing. Maybe your plex db is corrupted!" % row[1]
			continue

		if row2[0] == 1:	# movie
			filename = "%s/%s (%d)%s" % (readSectionLocation(row[3]), sanitizeFileName(row2[1]), row2[2].year, os.path.splitext(row[1])[1])

			if filename not in renamed_files_list:
				renamed_files_list.append(filename)
			else:
				print "FATAL: multiple entries with same destination file (%s)." % filename
				exit(1)


			if filename == row[1]:
				continue

			rename_table.append({
				"media_parts_id": row[2],
				"original_filename": row[1],
				"new_filename": filename
			})

			subtitle = checkSubtitle(row[1], filename)
			if subtitle:
				rename_table.append(subtitle)

		elif row2[0] == 4:	# tv episode
			season = getMetadataItem(row2[3])
			tvshow = getMetadataItem(season[3])
			
			filename = "%s/%s/Season %02d/%s - s%02de%02d - %s%s" % (
				readSectionLocation(row[3]),
				sanitizeFileName(tvshow[1]),
				season[2],
				sanitizeFileName(tvshow[1]),
				season[2],
				row2[4],
				sanitizeFileName(row2[1]),
				os.path.splitext(row[1])[1]
			)
			# filename = filename.replace('TV Shows/TV Cleanup','pTV')

			if filename not in renamed_files_list:
				renamed_files_list.append(filename)
			else:
				print "FATAL: multiple entries with same destination file (%s)." % filename
				exit(1)

			if filename == row[1]:
				continue

			rename_table.append({
				"media_parts_id": row[2],
				"original_filename": row[1],
				"new_filename": filename
			})
			
			subtitle = checkSubtitle(row[1], filename)
			if subtitle:
				rename_table.append(subtitle)

		elif row2[0] == 10:	# song
			album = getMetadataItem(row2[3])
			artist = getMetadataItem(album[3])
			
			filename = "%s/%s/%s/%02d - %s%s" % (
				readSectionLocation(row[3]),
				sanitizeFileName(artist[1]),
				sanitizeFileName(album[1]),
				row2[4],
				sanitizeFileName(row2[1]),
				os.path.splitext(row[1])[1]
			)

			if filename not in renamed_files_list:
				renamed_files_list.append(filename)
			else:
				print "FATAL: multiple entries with same destination file (%s)." % filename
				exit(1)

			if filename == row[1]:
				continue

			rename_table.append({
				"media_parts_id": row[2],
				"original_filename": row[1],
				"new_filename": filename
			})

	return rename_table
	
parser = argparse.ArgumentParser(description="Rename media on filesystem following the meta inside the plex database.")
actions = parser.add_mutually_exclusive_group()
actions.add_argument("-r", "--rename", metavar="N", type=int, nargs="+", help="rename files following the plex database")
actions.add_argument("-c", "--cleanup", metavar="N", type=int, nargs="+", help="clean unused file from filesystem")
actions.add_argument("-l", "--list", action="store_true", dest="list", default=False, help="list sections")

parser.add_argument(
	"-e",
	"--execute",
	action="store_true",
	dest="execute",
	default=False,
	help="with -r/--rename or -c/--cleanup option really rename/cleanup files (otherwise is like 'dry only')")
parser.add_argument("-d", "--database", metavar="DB", type=str, help="plex database (com.plexapp.plugins.library.db)")
parser.add_argument(
	"-m",
	"--move",
	nargs="+",
	dest="move",
	help="with -r/--rename option rename and move files to specified directory.")
args = vars(parser.parse_args())

if args["database"]:
	database_filename = args["database"]
else:
	database_filename = os.path.expanduser("~/Library/Application Support/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db")

plexdb = sqlite3.connect(database_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

if args["list"]:
	sections = readSections()
	for section in sections:
		locations = ""
		for location in section["locations"]:
			if len(locations) > 0:
				locations += ", "
			locations += location
		print "%s - %s (%s)" % (section["id"], section["name"], locations.strip())

elif args["rename"]:
	for section in args["rename"]:
		# print "section: %s" % (section)
		rename_table = guessSectionActions(section)
		for row in rename_table:
			print "rename: %s -> %s" % (row["original_filename"], row["new_filename"])
			if args["execute"]:
				try:
					os.makedirs(os.path.dirname(row["new_filename"]))
				except Exception, e:
					pass
				if row["media_parts_id"] != -1:
					plexdb.execute("UPDATE media_parts SET file=? WHERE id=?", (row["new_filename"], row["media_parts_id"]))
					plexdb.commit()
				os.rename(row["original_filename"], row["new_filename"])

elif args["cleanup"]:
	for section in args["cleanup"]:
		rm_table = guessSectionCleanup(section)
		for row in rm_table:
			if row["type"] == "file":
				print "remove file: %s" % row["filename"]
				if args["execute"]:
					try:
						os.unlink(row["filename"])
					except Exception, e:
						pass
			else:
				print "remove folder: %s" % row["path"]
				if args["execute"]:
					try:
						shutil.rmtree(row["path"])
					except Exception, e:
						pass
else:
	parser.print_help()
