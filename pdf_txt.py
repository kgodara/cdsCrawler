import fitz

doc = fitz.open('colleges/cornell/1000569.pdf')
n = 0
found = 0
pages_read = 0
while n < doc.pageCount:
	page = doc.loadPage(n)
	text = page.getText("text")
	print(str(text))
	n += 1