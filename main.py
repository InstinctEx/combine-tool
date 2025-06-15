import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime
import threading
# This script combines audio and video files in specified folders using FFmpeg.
# It logs the progress and results of each operation.
# FILES MUST BE NAMED "video" and "audio" in their filenames.
# Ensure you have FFmpeg installed and available in your PATH.
PARENT_DIR = "path/to/folder"
LOG_FILE = os.path.join(PARENT_DIR, "process_log.txt")

def log_message(msg):
    with open(LOG_FILE, "a") as log:
        log.write(f"{msg}\n")
    print(msg)

def get_duration(filepath):
    """Get video/audio duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", filepath],
            capture_output=True, text=True, check=True
        )
        return float(result.stdout.strip())
    except Exception as e:
        return None

def run_ffmpeg_with_progress(command, total_duration, folder_name):
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1
    )

    bar = tqdm(
        total=total_duration,
        desc=f"{folder_name}",
        unit="s",
        position=1,
        leave=False
    )

    out_time = 0.0
    while True:
        line = process.stdout.readline()
        if line == '':
            break
        if line.startswith("out_time_ms="):
            out_time = int(line.strip().split("=")[1]) / 1_000_000
            bar.n = min(out_time, total_duration)
            bar.refresh()
        elif line.strip() == "progress=end":
            break

    process.wait()
    bar.n = total_duration
    bar.refresh()
    bar.close()
    return process.returncode == 0

def combine_audio_video(folder_path):
    folder_name = os.path.basename(folder_path)
    files = os.listdir(folder_path)

    video_file = next((f for f in files if "video" in f.lower() and f.lower().endswith(".mp4")), None)
    audio_file = next((f for f in files if "audio" in f.lower() and f.lower().endswith(".mp4")), None)

    if not video_file or not audio_file:
        return f" Skipped {folder_name}: Missing audio or video file."

    input_video = os.path.join(folder_path, video_file)
    input_audio = os.path.join(folder_path, audio_file)
    output_file = os.path.join(folder_path, f"{folder_name}_combined.mp4")

    # Estimate duration from the video
    duration = get_duration(input_video)
    if not duration:
        return f" Failed to get duration for {folder_name}"

    command = [
        "ffmpeg",
        "-i", input_video,
        "-i", input_audio,
        "-c:v", "copy",
        "-c:a", "aac",
        "-strict", "experimental",
        "-y",
        "-progress", "pipe:1",
        "-loglevel", "error",
        output_file
    ]

    success = run_ffmpeg_with_progress(command, duration, folder_name)
    return f" Combined {folder_name} successfully." if success else f" Failed in {folder_name}"

def main():
    with open(LOG_FILE, "w") as log:
        log.write(f"FFmpeg muxing started at {datetime.now()}\n\n")

    folders = [
        os.path.join(PARENT_DIR, d) for d in os.listdir(PARENT_DIR)
        if os.path.isdir(os.path.join(PARENT_DIR, d))
    ]

    log_message(f"Found {len(folders)} folders to process.\n")

    results = []
    with ThreadPoolExecutor(max_workers=2) as executor:  # Limit parallelism to avoid terminal clutter
        futures = {executor.submit(combine_audio_video, folder): folder for folder in folders}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Overall Progress", position=0):
            result = future.result()
            log_message(result)
            results.append(result)

    log_message(f"\nAll done at {datetime.now()}.")

if __name__ == "__main__":
    main()
