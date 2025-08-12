import sharp from "sharp";

const resizeAndCompress = async (req, res, next) => {
  if (!req.files || !Array.isArray(req.files)) return next();

  await Promise.all(
    req.files.map(async (file) => {
      let quality = 80;
      let buffer = await sharp(file.buffer)
        .resize(1000, 1000, { fit: "cover" })
        .jpeg({ quality })
        .toBuffer();

      // Keep compressing until under 200kb or quality is too low
      while (buffer.length > 200 * 1024 && quality > 30) {
        quality -= 10;
        buffer = await sharp(file.buffer)
          .resize(1000, 1000, { fit: "cover" })
          .jpeg({ quality })
          .toBuffer();
      }

      file.buffer = buffer;
      file.size = buffer.length;
      file.mimetype = "image/jpeg";
    })
  );

  next();
};

export default resizeAndCompress;
