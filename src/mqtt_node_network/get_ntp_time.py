import ntplib
from time import ctime

# Note that this module is not currently used in the project
# It is included here as an example of how to get NTP time
# It is intended to be used for future development


def get_ntp_time(server="pool.ntp.org"):
    try:
        client = ntplib.NTPClient()
        response = client.request(server, version=3)
        # Return the NTP time with the highest accuracy
        return {
            "unix_time": response.tx_time,  # Time in seconds since epoch
            "formatted_time": ctime(response.tx_time),  # Human-readable time
            "offset": response.offset,  # Offset between server and local clock
            "stratum": response.stratum,  # Stratum level (indicates the server accuracy)
            "delay": response.delay,  # Round trip delay in seconds
        }
    except Exception as e:
        return f"Failed to get NTP time: {e}"


if __name__ == "__main__":
    ntp_time = get_ntp_time()
    print(ntp_time)
