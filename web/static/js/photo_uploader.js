window.photoUploader = function ({ maxFiles, maxSizeMB }) {
  return {
    files: [],        // active + new files
    deleted: [],      // deleted existing photos

    init() {
      const el = document.getElementById("photos-data");
      const initialPhotos = el ? JSON.parse(el.textContent) : [];
      for (const p of initialPhotos) {
        this.files.push({
          id: p.id,
          file: null,
          url: p.url,
          name: p.name,
          bump: false,
        });
      }
    },

    validateFiles(file) {
      if (!(file instanceof File)) return false;
      if (file.size === 0) return false;
      if (!file.type.startsWith("image/")) {
        alert(`"${file.name}" is not an image file`);
        return false;
      }
      if (file.size > maxSizeMB * 1024 * 1024) {
        alert(`"${file.name}" is too large (max ${maxSizeMB}MB)`);
        return false;
      }
      if (this.files.some(f => (f.file?.name || f.name) === file.name)) {
        alert(`"${file.name}" is already added`);
        return false;
      }
      if (this.files.length >= maxFiles) {
        alert(`Maximum ${maxFiles} files allowed`);
        return false;
      }
      return true;
    },

    handleFiles(fileList) {
      const originalLength = this.files.length;
      
      for (const file of fileList) {
        if (!this.validateFiles(file)) continue;
        
        this.files.push({
          id: Math.random().toString(36).slice(2), // just to avoid id colision
          file,
          url: null,
          previewUrl: URL.createObjectURL(file),
          name: file.name,
          bump: true
        });
      }

      if (this.files.length > originalLength) {
        this.syncInput();
        this.bump();
      }
    },

    handleDrop(event) {
      event.preventDefault();
      const files = event.dataTransfer.files;
      if (!files || !files.length) return;
      this.handleFiles(files);
    },

    move(index, direction) {
      const newIndex = index + direction;
      if (newIndex < 0 || newIndex >= this.files.length) return;
      
      this.files[index].bump = true;
      [this.files[index], this.files[newIndex]] = 
        [this.files[newIndex], this.files[index]];
      this.bump();
    },

    moveUp(index) {
      this.move(index, -1);
    },

    moveDown(index) {
      this.move(index, 1);
    },

    remove(index) {
      if (!confirm("Remove this photo?")) return;
      const obj = this.files[index];
      if (obj.previewUrl && obj.previewUrl.startsWith('blob:')) {
        URL.revokeObjectURL(obj.previewUrl);
      }
      if (obj.file == null) this.deleted.push(obj); 
      this.files.splice(index, 1);
      this.syncInput();
    },

    displayName(obj) {
      return obj.file?.name || obj.name || "Unnamed photo";
    },

    previewUrl(obj) {
      return obj.previewUrl || obj.url;
    },

    serializePhotos(filterFn) {
      return JSON.stringify(
        this.files
          .filter(filterFn)
          .map(obj => {
            const order = this.files.indexOf(obj);
            return {
              id: obj.id,
              order: order,
              is_primary: order === 0
            };
          })
      );
    },

    serializedAdd() {
      return this.serializePhotos(obj => obj.file !== null);
    },

    serializedUpdate() {
      return this.serializePhotos(obj => obj.file === null);
    },

    serializedDeleted() {
      return JSON.stringify(this.deleted.map(obj => ({
        id: obj.id,
        mark_delete: true
      })));
    },

    bump() {
      this.$nextTick(() => this.files.forEach(f => delete f.bump));
    },

    syncInput() {
      const input = this.$refs.input;
      if (!input) return;
      
      const dt = new DataTransfer();
      this.files.forEach(obj => {
        if (obj.file instanceof File) dt.items.add(obj.file);
      });
      input.files = dt.files;
    },

    destroy() {
      this.files.forEach(file => {
        if (file.previewUrl && file.previewUrl.startsWith('blob:')) {
          URL.revokeObjectURL(file.previewUrl);
        }
      });
    }

  };
};