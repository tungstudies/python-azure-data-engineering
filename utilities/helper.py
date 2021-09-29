import time


class ColorPrinter:
    @classmethod
    def pr_red(cls, text: str): print(f"\033[91m{text}\033[00m")

    @classmethod
    def pr_green(cls, text: str): print(f"\033[92m{text}\033[00m")

    @classmethod
    def pr_yellow(cls, text: str): print(f"\033[93m{text}\033[00m")

    @classmethod
    def pr_light_purple(cls, text: str): print(f"\033[94m{text}\033[00m")

    @classmethod
    def pr_purple(cls, text: str): print(f"\033[95m{text}\033[00m")

    @classmethod
    def pr_cyan(cls, text: str): print(f"\033[96m{text}\033[00m")

    @classmethod
    def pr_light_gray(cls, text: str): print(f"\033[97m {text}\033[00m")

    @classmethod
    def pr_blue(cls, text: str): print(f"\r\033[94m{text}\033[00m")


def countdown(message: str, time_sec):
    while time_sec:
        mins, secs = divmod(time_sec, 60)
        time_format = '{:02d}:{:02d}'.format(mins, secs)
        if mins or secs:
            print(f'\r\033[94m{message}: {time_format}\033[0m', end='', flush=True)
        time.sleep(1)
        time_sec -= 1
    print(f'\r ', end='', flush=True)