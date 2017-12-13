import tkinter as tk
from PIL import Image, ImageTk
import random
import getRecommendations as gr


root = tk.Tk()


background_image=ImageTk.PhotoImage(Image.open('superior-som-logo-sm.jpg'))
background_label = tk.Label(root, image=background_image)
background_label.grid(row = 0, column = 4)
# background_label.place(x=0, y=0, relwidth=1, relheight=1)

entry_label = tk.Label(root, text = "Enter the Name of a Bottle of Wine: ")
entry_label.grid(row = 0, column = 0)

entry_label = tk.Label(root, text = "Top 10 Recommendations: ")
entry_label.grid(row = 2, column = 1)

entry_label = tk.Label(root, text = "Query: ")
entry_label.grid(row = 1, column = 0)

entry_label = tk.Label(root, text = "1: ")
entry_label.grid(row = 3, column = 0)

entry_label = tk.Label(root, text = "2: ")
entry_label.grid(row = 4, column = 0)

entry_label = tk.Label(root, text = "3: ")
entry_label.grid(row = 5, column = 0)

entry_label = tk.Label(root, text = "4: ")
entry_label.grid(row = 6, column = 0)

entry_label = tk.Label(root, text = "5: ")
entry_label.grid(row = 7, column = 0)

entry_label = tk.Label(root, text = "6: ")
entry_label.grid(row = 8, column = 0)

entry_label = tk.Label(root, text = "7: ")
entry_label.grid(row = 9, column = 0)

entry_label = tk.Label(root, text = "8: ")
entry_label.grid(row = 10, column = 0)

entry_label = tk.Label(root, text = "9: ")
entry_label.grid(row = 11, column = 0)

entry_label = tk.Label(root, text = "10: ")
entry_label.grid(row = 12, column = 0)


#Entry field for user guesses.
user_entry = tk.Entry(root)
user_entry.grid(row = 0, column = 1)


def recs_to_string(event = None):
	guess = user_entry.get()

	recs_df = gr.getRecs(guess)
	# recs_df = recs_df.drop('query', 1)
	recs_list = recs_df.values.tolist()
	# recs_string = recs_df.to_string(index = False, header = False, col_space = 200)

	text_box0 = tk.Text(root, width = 80, height = 2)
	text_box0.grid(row = 1, column = 1, columnspan = 2)
	text_box0.insert("end-1c", recs_list[0][0])

	text_box1 = tk.Text(root, width = 80, height = 2)
	text_box1.grid(row = 3, column = 1, columnspan = 2)
	text_box1.insert("end-1c", recs_list[0][1])

	text_box2 = tk.Text(root, width = 80, height = 2)
	text_box2.grid(row = 4, column = 1, columnspan = 2)
	text_box2.insert("end-1c", recs_list[0][2])

	text_box3 = tk.Text(root, width = 80, height = 2)
	text_box3.grid(row = 5, column = 1, columnspan = 2)
	text_box3.insert("end-1c", recs_list[0][3])

	text_box4 = tk.Text(root, width = 80, height = 2)
	text_box4.grid(row = 6, column = 1, columnspan = 2)
	text_box4.insert("end-1c", recs_list[0][4])

	text_box5 = tk.Text(root, width = 80, height = 2)
	text_box5.grid(row = 7, column = 1, columnspan = 2)
	text_box5.insert("end-1c", recs_list[0][5])

	text_box6 = tk.Text(root, width = 80, height = 2)
	text_box6.grid(row = 8, column = 1, columnspan = 2)
	text_box6.insert("end-1c", recs_list[0][6])

	text_box7 = tk.Text(root, width = 80, height = 2)
	text_box7.grid(row = 9, column = 1, columnspan = 2)
	text_box7.insert("end-1c", recs_list[0][7])

	text_box8 = tk.Text(root, width = 80, height = 2)
	text_box8.grid(row = 10, column = 1, columnspan = 2)
	text_box8.insert("end-1c", recs_list[0][8])

	text_box9 = tk.Text(root, width = 80, height = 2)
	text_box9.grid(row = 11, column = 1, columnspan = 2)
	text_box9.insert("end-1c", recs_list[0][9])

	text_box10 = tk.Text(root, width = 80, height = 2)
	text_box10.grid(row = 12, column = 1, columnspan = 2)
	text_box10.insert("end-1c", recs_list[0][10])

# random_num = random.randint(1, 5)
#
# def guess_number(event = None):
#     #Get the string of the user_entry widget
#     guess = user_entry.get()
#
#     if guess == str(random_num):
#         text_box.delete(1.0, "end-1c") # Clears the text box of data
#         text_box.insert("end-1c", "You win!") # adds text to text box
#
#     else:
#         text_box.delete(1.0, "end-1c")
#         text_box.insert("end-1c", "Try again!")
#
#         user_entry.delete(0, "end")
# # binds the enter widget to the guess_number functionwhile the focus/cursor is on the user_entry widget
user_entry.bind("<Return>", recs_to_string)

root.mainloop()
