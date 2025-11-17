import requests
import time
import random
import string
from PIL import Image
import base64
import io

url = "https://kirkify.net/api/kirkify"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6IjdJQVdSclEwR29kZ1dkOCsiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL25xZGVla3h0bWR5cXZ6YnF3dWtyLnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiI0ZDZlMGE2YS1hMjEwLTQxOTgtYTQzMC1hZDFmMmI3YjE1NDgiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzYzMzkxNjE4LCJpYXQiOjE3NjMzODgwMTgsImVtYWlsIjoicmFvdWZpLnNhaWRtYXNvb21AZ21haWwuY29tIiwicGhvbmUiOiIiLCJhcHBfbWV0YWRhdGEiOnsicHJvdmlkZXIiOiJnb29nbGUiLCJwcm92aWRlcnMiOlsiZ29vZ2xlIl19LCJ1c2VyX21ldGFkYXRhIjp7ImF2YXRhcl91cmwiOiJodHRwczovL2xoMy5nb29nbGV1c2VyY29udGVudC5jb20vYS9BQ2c4b2NKaHQ5eWNVYmN6b0t1YTVUNEZ3Y0JlbzlvTnRmc3BPMTNDNFJ4UHlHN1BoOFJObVE9czk2LWMiLCJlbWFpbCI6InJhb3VmaS5zYWlkbWFzb29tQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJmdWxsX25hbWUiOiJNYXNvb20iLCJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJuYW1lIjoiTWFzb29tIiwicGhvbmVfdmVyaWZpZWQiOmZhbHNlLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EvQUNnOG9jSmh0OXljVWJjem9LdWE1VDRGd2NCZW85b050ZnNwTzEzQzRSeFB5RzdQaDhSTm1RPXM5Ni1jIiwicHJvdmlkZXJfaWQiOiIxMTAyMjUzNTQxMDA2MDcyMzE1OTIiLCJzdWIiOiIxMTAyMjUzNTQxMDA2MDcyMzE1OTIifSwicm9sZSI6ImF1dGhlbnRpY2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJvYXV0aCIsInRpbWVzdGFtcCI6MTc2MzM4ODAxOH1dLCJzZXNzaW9uX2lkIjoiYTIyNjZiZmQtZTQzYi00NmM0LTkwMjEtOTU1NzIyYTQ1NTU5IiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.Q32_CvzgyL_hNPV3HYci8TSfz5CA9nG4bKy42wL2jPs",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Origin": "https://kirkify.net",
    "Referer": "https://kirkify.net/generator",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-CH-UA": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"'
}

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_fake_image_base64():
    # Create a fake image
    image = Image.new('RGB', (100, 100), color = (73, 109, 137))
    buf = io.BytesIO()
    image.save(buf, format='JPEG')
    byte_im = buf.getvalue()
    return base64.b64encode(byte_im).decode('utf-8')

data = {
    "fingerprint": generate_random_string(32),
    "imageData": f"data:image/jpeg;base64,{generate_fake_image_base64()}"
}

while True:
    print(f"Sending request with data: {data}")
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 429:
        print("Rate limited, waiting for a while...")
        time.sleep(random.uniform(0.5, 1.5))
    else:
        print(f"Response Status: {response.status_code}")
        print(f"Response Text: {response.text}")
    time.sleep(random.uniform(0.05, 0.15))