#This is a small program for generating barcode image files that can be sent to marketing and added to coupons and other materials.
#
#The SVG file format works well, as its a mechanical specification for drawing images - it upscales more reliably than something like .jpeg.
#If you would like to save the file in a format other than SVG, simply change the import from barcode.writer to ImageWriter instead of SVGWriter,
#then uncomment (remove the leading # characters) the relevant ImageWriter lines and toggle the 'format' parameter to your desired format.
#You may need to play with getting the format call correct, e.g. ImageWriter(format='jpeg') vs ImageWriter(format='JPEG') vs ImageWriter(format='JPG')

#Library Tutorial and Documentation:
#https://python-barcode.readthedocs.io/en/latest/getting-started.html
#https://python-barcode.readthedocs.io/en/stable/

import barcode
from barcode.writer import SVGWriter

#12 digits long, !!including the checksum!!

#FOR DAN'S ORDER BOOK - INCLUDES CHECKSUM WITHOUT THE DASH, 14 DIGITS TOTAL
#ten_off_forty_code = barcode.Gs1_128('00026938122225', writer=SVGWriter())



#ten_off_forty_code = barcode.get_barcode_class('UPC')('', writer=SVGWriter())

#ten_off_forty_code.save("testBulk")

# fiveFor25Code = barcode.get_barcode_class('UPC')('00000000555', writer=SVGWriter())
# deliOneOffCode = barcode.get_barcode_class('UPC')('00000000556', writer=SVGWriter())

# fiveFor25Code.save("GW5for25")
# deliOneOffCode.save("GWDeliOneOff")

# fiveFor25Code = barcode.get_barcode_class('UPC')('00000000555', writer=ImageWriter(format='gif'))
# deliOneOffCode = barcode.get_barcode_class('UPC')('00000000556', writer=ImageWriter(format='gif'))

# fiveFor25Code.save("GW5for25")
# deliOneOffCode.save("GWDeliOneOff")


#The way this works is you're going to set up a new coupon in SMS with an in-house "UPC"
# 1. Choose an arbitrary number that isn't already associated with a UPC in SMS, three to four digits long is likely best
# 1a (optional). Duplicate the coupon with SMS UPC number 55 if you're looking to do a $ off some $ coupon store wide,
#                       you'll have to manually re-add the sub-depts to exclude beer and wine.
# 2. Whatever you choose for the "UPC", enter it below as a string argument inside of a ***11 character long*** string with leading zeros on the left
#               [00000000###] or [000000000##] or [000000#####] etc., where # = your unique digits
#   !!!!IT MUST BE 11 CHARACTERS LONG OR THE BARCODE LIBRARY WITH OVERWRITE YOUR DIGITS WITH A CHECKSUM!!!!
#   This has been double checked multiple times, and you need the barcode library to add a checksum onto the end of the barcode for the register scanners to successfully read the code,
#   If the string you give is longer than 11 characters, the barcode will not line up with what you have in SMS
# 3. Give you save file name target a new name, it will save into \\bfc-hv-01\SWAP\PythonScripts\
coupon = barcode.UPCA('00000000925', writer=SVGWriter())
coupon.save("email_coupon")