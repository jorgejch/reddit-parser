#!/usr/bin/env python

"""
Simple "Square Detector" program.
Loads several images sequentially and tries to find squares in each image.
source: https://github.com/opencv/opencv/blob/master/samples/python/squares.py
"""
import sys
import urllib3
import numpy as np
import cv2 as cv
import vector as vector


def angle_cos(p0, p1, p2):
    d1, d2 = (p0 - p1).astype('float'), (p2 - p1).astype('float')
    return abs(np.dot(d1, d2) / np.sqrt(np.dot(d1, d1) * np.dot(d2, d2)))


def find_squares(img):
    print("Blurring image.")
    img = cv.GaussianBlur(img, (5, 5), 0)
    squares = []
    for gray in cv.split(img):
        for thrs in range(0, 255, 2):
            if thrs == 0:
                bin = cv.Canny(gray, 0, 25, apertureSize=5)
                bin = cv.dilate(bin, cv.getStructuringElement(cv.MORPH_RECT, (3, 3)))
            else:
                print("Processing Non-maximum Suppression.")
                _retval, bin = cv.threshold(gray, thrs, 255, cv.THRESH_BINARY_INV)
            print("Finding contours.")
            contours, _hierarchy = cv.findContours(bin, cv.RETR_LIST, cv.CHAIN_APPROX_SIMPLE)
            for cnt in contours:

                print("Producing square.")
                cnt_len = cv.arcLength(cnt, True)
                cnt = cv.approxPolyDP(cnt, 0.02 * cnt_len, True)
                if len(cnt) == 4 and cv.contourArea(cnt) > 1000 and cv.isContourConvex(cnt):
                    cnt = cnt.reshape(-1, 2)
                    max_cos = np.max([angle_cos(cnt[i], cnt[(i + 1) % 4], cnt[(i + 2) % 4]) for i in range(4)])
                    if max_cos < 0.1:
                        squares.append(cnt)
                        print("Produced {} squares.".format(len(squares)))
    return squares


def main(img_url):
    http = urllib3.PoolManager()
    img = http.request('GET', img_url)
    img_file = open('orig_file.jpg', 'bw')
    img_file.write(img.data)
    img_file.close()
    # jpg_as_np = np.frombuffer('orig_file.png', dtype=np.uint8)
    # img = cv.imdecode(jpg_as_np, flags=1)
    cv_img = cv.imread('orig_file.jpg', cv.IMREAD_REDUCED_COLOR_4)
    squares = find_squares(cv_img)
    print("About to draw contours   .")
    cv.drawContours(cv_img, squares, -1, (0, 255, 0), 3)
    print("Contours drawn.")
    # squared_file = open('squared.png', 'wb')
    # compression_params = 'IMWRITE_JPEG_QUALITY', 95
    cv.imwrite('squared_image.png', cv_img)
    # squared_file.close()
    # cv.imshow('squares', cv_img)
    # ch = cv.waitKey()
    # print(ch)
    # if ch == 27:
    #     return


if __name__ == '__main__':
    # print(__doc__)
    main(sys.argv[1])
    # cv.destroyAllWindows()
