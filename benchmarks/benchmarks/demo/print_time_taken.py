def _print_time_taken(begin, end):

    seconds = end - begin

    hours = int(seconds / 3600)
    seconds -= float(hours * 3600)

    minutes = int(seconds / 60)
    seconds -= float(minutes * 60)

    seconds = round(seconds, 6)

    print("Time taken: " + str(hours) + "h:" + str(minutes) + "m:" + str(seconds))

    print("")
