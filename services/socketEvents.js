// socketEvents.js
let ioInstance = null;

export const registerSocketIO = (io) => {
  ioInstance = io;
};

export const emitTagEvent = (tagName) => {
  if (ioInstance) {
    ioInstance.emit("new_tag", { tag: tagName });
    console.log("Emitted new_tag:", tagName);
  }
};

export const emitGroupEvent = (group) => {
  if (ioInstance) {
    ioInstance.emit("new_group", group);
    console.log("Emitted new_group:", group);
  }
};

export const emitCollectionCreateEvent = (collection) => {
  if (ioInstance) {
    ioInstance.emit("collection_created", { collectionData: collection });
    console.log("Emitted collection_created:", collection);
  }
};

export const emitCollectionUpdateEvent = (collection) => {
  if (ioInstance) {
    ioInstance.emit("collection_updated", { collectionData: collection });
    console.log("Emitted collection_updated:", collection);
  }
};

export const emitCollectionDeleteEvent = (collection) => {
  if (ioInstance) {
    ioInstance.emit("collection_deleted", { collectionData: collection });
    console.log("Emitted collection_deleted:", collection);
  }
};
