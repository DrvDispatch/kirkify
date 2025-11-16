import os
from tkinter import *
from tkinter import filedialog
from PIL import Image, ImageTk

class ImageViewer:
    def __init__(self, root):
        self.root = root
        self.root.title("Folder Image Visualizer")
        self.root.geometry("1200x700")

        # Frame for folder list
        self.left_frame = Frame(root, width=300)
        self.left_frame.pack(side=LEFT, fill=Y)

        # Frame for image display
        self.right_frame = Frame(root)
        self.right_frame.pack(side=RIGHT, fill=BOTH, expand=True)

        # Folder list
        self.folder_listbox = Listbox(self.left_frame, width=45)
        self.folder_listbox.pack(fill=Y, expand=True)
        self.folder_listbox.bind("<<ListboxSelect>>", self.show_images)

        # Button to select parent directory
        Button(self.left_frame, text="Select Parent Folder", command=self.load_parent_folder).pack(pady=10)

        # Canvas for images
        self.canvas = Canvas(self.right_frame, bg="white")
        self.canvas.pack(side=LEFT, fill=BOTH, expand=True)

        # Scrollbar
        self.scrollbar = Scrollbar(self.right_frame, orient=VERTICAL, command=self.canvas.yview)
        self.scrollbar.pack(side=RIGHT, fill=Y)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.image_frame = Frame(self.canvas)
        self.canvas.create_window((0, 0), window=self.image_frame, anchor="nw")

        self.image_refs = []  # Prevent garbage collection of images

    def load_parent_folder(self):
        folder = filedialog.askdirectory()
        if not folder:
            return

        self.parent_folder = folder

        self.folder_listbox.delete(0, END)
        for name in os.listdir(folder):
            path = os.path.join(folder, name)
            if os.path.isdir(path):
                self.folder_listbox.insert(END, name)

    def show_images(self, event=None):
        if not hasattr(self, 'parent_folder'):
            return
        
        selection = self.folder_listbox.curselection()
        if not selection:
            return

        folder_name = self.folder_listbox.get(selection[0])
        folder_path = os.path.join(self.parent_folder, folder_name)

        # Clear previous images
        for widget in self.image_frame.winfo_children():
            widget.destroy()
        self.image_refs.clear()

        allowed_ext = (".png", ".jpg", ".jpeg", ".bmp", ".gif")

        images = [f for f in os.listdir(folder_path) if f.lower().endswith(allowed_ext)]

        if not images:
            Label(self.image_frame, text="No images found", font=("Arial", 16)).pack()
            return

        # Show images
        for img_name in images:
            img_path = os.path.join(folder_path, img_name)

            img = Image.open(img_path)
            img.thumbnail((300, 300))
            photo = ImageTk.PhotoImage(img)

            lbl = Label(self.image_frame, image=photo)
            lbl.pack(pady=10)

            self.image_refs.append(photo)

        self.canvas.update_idletasks()
        self.canvas.config(scrollregion=self.canvas.bbox("all"))


if __name__ == "__main__":
    root = Tk()
    app = ImageViewer(root)
    root.mainloop()
