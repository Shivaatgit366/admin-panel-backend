import axios from "axios";
import { tryCatchFn } from "./errorController.js";
import { metalKeys } from "../helpers/commonConstHelper.js";
import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import {
  qualityMapping,
  stullerCategoryIds,
} from "../helpers/commonConstHelper.js";
import dotenv from "dotenv";
dotenv.config();

const insertStullerInDb = tryCatchFn(async (req, res) => {
  const USERNAME = process.env.STULLER_USER;
  const PASSWORD = process.env.STULLER_PASS;
  const BASE_URL = "https://api.stuller.com/v2/products";
  const encodedCredentials = Buffer.from(`${USERNAME}:${PASSWORD}`).toString(
    "base64"
  );

  // This will store the token returned by the API for the next page
  let nextPage = null;

  // This array will hold all the product data collected from all pages
  let allStullerData = [];

  // Fetch API Data
  do {
    let requestBody = {};

    // If nextPage token exists, only include it in the request body
    if (nextPage) {
      requestBody = {
        NextPage: nextPage,
      };
    } else {
      // Initial first request without nextPage, send full filter and category information;
      requestBody = {
        Include: ["All"],
        Filter: ["Finished"],
        CategoryIds: stullerCategoryIds,
      };
    }

    const apiResponse = await axios.post(BASE_URL, requestBody, {
      headers: {
        Authorization: `Basic ${encodedCredentials}`,
        "Content-Type": "application/json",
      },
    });

    const responseData = apiResponse.data;

    // If Products are present in the response, add them to the array
    if (
      responseData &&
      Array.isArray(responseData.Products) &&
      responseData.Products.length > 0
    ) {
      allStullerData.push(...responseData.Products);
    }

    // Update the nextPage token for the next loop iteration
    nextPage = responseData.NextPage;
  } while (nextPage);

  console.log("total data fetched from Stuller API:", allStullerData.length);

  // Store the category IDs in a Set for quick lookup
  const stullerCategorySet = new Set(stullerCategoryIds);

  // ✅ Transform API Data
  const transformData = (data) => {
    const validMetals = new Set([
      "14Ky",
      "14Kw",
      "14Kr",
      "18Ky",
      "18Kw",
      "18Kr",
      "Plat",
    ]);
    const validStatuses = new Set(["Made To Order", "In Stock"]);
    const validStones = new Set(["Natural Diamond", "Lab-Grown Diamond", "NS"]);
    const result = {};
    const validSKU = new Set();

    data.forEach((item) => {
      const elements = item.DescriptiveElementGroup.DescriptiveElements;
      const metalType = elements
        .find((el) => el.Name === "Quality")
        ?.Value?.trim();
      const groupId =
        elements.find((el) => el.Name === "SERIES")?.Value?.trim() ||
        elements.find((el) => el.Name === "Series")?.Value?.trim();
      const productType = elements
        .find((el) => el.Name === "Product")
        ?.Value?.trim();
      const jewelryState = elements
        .find((el) => el.Name === "Jewelry State")
        ?.Value?.trim();

      // if (!groupId || !metalType || !item.SKU || !validMetals.has(metalType) || !validStatuses.has(item.Status) || productType !== "Band" || jewelryState !== "Set") {
      //     return;
      // }
      if (
        !groupId ||
        !metalType ||
        !item.SKU ||
        !validMetals.has(metalType) ||
        !validStatuses.has(item.Status) ||
        productType !== "Band" ||
        !(jewelryState === "Set" || jewelryState === "N/A")
      ) {
        return;
      }

      if (!result[groupId]) {
        result[groupId] = {
          groupId,
          // groupName: "",
          WebCategories:
            item.WebCategories?.filter(({ Id }) =>
              stullerCategorySet.has(Id)
            ) || [],
          stoneList: new Set(),
          metalList: new Set(),
          metal: {},
        };
      }

      // Extract primaryStoneType and set "N/A" if not present
      // let primaryStoneType = item.Specifications?.find(spec => spec.Name === "Primary Stone Type")?.Value?.trim() || elements.find(el => el.Name === "Primary Stone Type")?.Value?.trim() || "NS";
      let primaryStoneType = (
        item.Specifications?.find(
          ({ Name }) => Name === "Primary Stone Type"
        )?.Value?.trim() ||
        elements
          .find(({ Name }) => Name === "Primary Stone Type")
          ?.Value?.trim() ||
        "NS"
      ).replace(/^N\/A$/, "NS");

      // Ensure primaryStoneType is valid
      if (!validStones.has(primaryStoneType)) {
        return; // Skip invalid stone types
      }

      if (!result[groupId].metal[metalType]) {
        result[groupId].metal[metalType] = {};
        result[groupId].metalList.add(metalType);
      }

      // ✅ Ensure only one of each stone type per metal
      if (result[groupId].metal[metalType][primaryStoneType]) return;

      // Add the stone type to the stone list
      result[groupId].stoneList.add(primaryStoneType);

      result[groupId].metal[metalType][primaryStoneType] = {
        Id: item.Id,
        Title: item.Description || "",
        SKU: item.SKU,
        Stuller_p_id: item.SKU,
        Description: item.ShortDescription || "",
        GroupDescription: item.GroupDescription || "",
        // WebCategories: item.WebCategories || [],
        Status: item.Status || "",
        SupplierPrice: Math.round(item.Price?.Value) || 0,
        SupplierShowcasePrice: Math.round(item.ShowcasePrice?.Value) || 0,
        GramWeight: item.GramWeight || 0,
        RingSize: item.RingSize || 0,
        LeadTime: item.LeadTime || 0,
        OnHand: item.OnHand || 0,
        Orderable: item.Orderable,
        CurrencyCode: item.Price?.CurrencyCode || "",
        Width:
          item.Specifications?.find(
            (spec) => spec.Name === "Approx. Shank Base Width"
          )?.Value || "",
        // PrimaryStoneType: primaryStoneType,
        // PrimaryStoneShape: item.Specifications?.find(spec => spec.Name === "Primary Stone Shape")?.Value?.trim() || elements.find(el => el.Name === "Primary Stone Shape")?.Value?.trim() || "",
        // SecondaryStoneType: item.Specifications?.find(spec => spec.Name === "Secondary Stone Type")?.Value || elements.find(el => el.Name === "Secondary Stone Type")?.Value || "N/A",
        // "Diamond CTW": item.Specifications?.find(spec => spec.Name === "Diamond CTW")?.Value || "",
        // Clarity: item.Specifications?.find(spec => spec.Name === "Diamond Clarity")?.Value || "",
        // Color: item.Specifications?.find(spec => spec.Name === "Diamond Color")?.Value || "",
        PrimaryStoneType: primaryStoneType,
        Quality:
          qualityMapping[
            elements
              .find(({ Name }) => Name === "Quality")
              ?.DisplayValue?.trim()
          ] || "",
        SetWith: item.SetWith || null,
        Sync: false,
        SyncId: "",
        VariantSyncId: "",
      };

      validSKU.add(item.SKU);
    });

    const transformedData = Object.values(result).map((group) => ({
      ...group,
      stoneList: Array.from(group.stoneList),
      metalList: Array.from(group.metalList),
    }));

    return { transformedData, validSKU };
  };

  const { transformedData, validSKU } = transformData(allStullerData);
  // Write the response to the file incrementally

  const insertOrUpdateData = async (transformedData) => {
    const connection = await sqldb.getConnection(); // Get connection from pool

    try {
      await connection.beginTransaction(); // Start transaction

      // ✅ Insert Rings if not exist
      await connection.query(
        `INSERT IGNORE INTO stlr_rings (supplier_group_id) VALUES ?`,
        [transformedData.map((r) => [r.groupId])]
      );

      // ✅ Fetch ring IDs
      const ringIdsMap = new Map();
      const [ringRows] = await connection.query(
        "SELECT supplier_group_id, ring_id FROM stlr_rings"
      );
      ringRows.forEach((row) =>
        ringIdsMap.set(row.supplier_group_id, row.ring_id)
      );

      // ----------------------------------------------
      // ✅ Insert Web Categories
      const WebCategoriesToInsert = new Map();
      const RingCategoryRelations = [];

      for (const data of transformedData) {
        const ringId = ringIdsMap.get(data.groupId);

        for (const cat of data.WebCategories) {
          if (!WebCategoriesToInsert.has(cat.Id)) {
            WebCategoriesToInsert.set(cat.Id, [
              cat.Id,
              cat.Name,
              cat.Path,
              cat.CategoryImageUrl,
            ]);
          }
          RingCategoryRelations.push([ringId, cat.Id]);
        }
      }

      console.log("WebCategoriesToInsert", WebCategoriesToInsert.size);
      console.log("RingCategoryRelations", RingCategoryRelations.length);

      if (WebCategoriesToInsert.size > 0) {
        const insertQuery = `
                INSERT IGNORE INTO stlr_webcategories (web_cat_id, name, path, image_url)
                VALUES ?
            `;
        await connection.query(insertQuery, [
          Array.from(WebCategoriesToInsert.values()),
        ]);
      }

      if (RingCategoryRelations.length > 0) {
        const insertRelationQuery = `
                INSERT IGNORE INTO stlr_ring_has_categories (ring_id, web_cat_id)
                VALUES ?
            `;
        await connection.query(insertRelationQuery, [RingCategoryRelations]);
      }

      // ----------------------------------------------

      // ✅ Insert Metals if not exist
      const metalFullNames = [
        ...new Set(
          transformedData
            .flatMap((d) => d.metalList)
            .map((key) => metalKeys[key])
            .filter(Boolean)
        ),
      ];

      // Prepare values for insertion (name, slug)
      const metalValues = metalFullNames.map((name) => [
        name,
        name.toLowerCase(),
      ]);

      // Insert full values
      await connection.query(
        `INSERT IGNORE INTO stlr_metals (name, slug) VALUES ?`,
        [metalValues]
      );

      // ✅ Fetch metal IDs
      const metalIdsMap = new Map();
      const [metalRows] = await connection.query(
        "SELECT name, metal_id FROM stlr_metals"
      );
      metalRows.forEach((row) => metalIdsMap.set(row.name, row.metal_id));

      // ✅ Insert Stones if not exist
      const stoneNames = [
        ...new Set(transformedData.flatMap((d) => d.stoneList)),
      ];
      await connection.query(
        `INSERT IGNORE INTO stlr_stones (name, slug) VALUES ?`,
        [stoneNames.map((s) => [s, s.toLowerCase()])]
      );

      // ✅ Fetch stone IDs
      const stoneIdsMap = new Map();
      const [stoneRows] = await connection.query(
        "SELECT name, stone_id FROM stlr_stones"
      );
      stoneRows.forEach((row) => stoneIdsMap.set(row.name, row.stone_id));

      // ✅ Fetch existing variations
      const existingVariationsMap = new Map();
      const [existingVariations] = await connection.query(
        "SELECT variation_id, sku, supplier_price, supplier_showcase_price, ring_id, sync_id, variant_sync_id FROM stlr_ring_variations"
      );
      existingVariations.forEach((row) =>
        existingVariationsMap.set(row.sku, row)
      );

      // ✅ Prepare sku sync set for shopify product sync
      const [rows] = await connection.query(
        "SELECT sku FROM stlr_ring_variations WHERE sync = true"
      );
      const skuSyncSet = new Set(rows.map((row) => row.sku));

      console.log(skuSyncSet);

      // ✅ Prepare lists for insert, update, and delete operations
      const variationsToInsert = [];
      const variationsToUpdate = [];
      const currentSKUs = new Set();
      const insertVariationSKU = new Set();
      const skuToVariationMap = new Map();
      const shopifyVariationsToUpdate = [];
      const syncIdsToDelete = new Set();

      // ✅ Process transformed data
      for (const data of transformedData) {
        const ringId = ringIdsMap.get(data.groupId);
        for (const [metalName, stoneData] of Object.entries(data.metal)) {
          const metalId = metalIdsMap.get(metalKeys[metalName]);
          for (const [stoneName, variation] of Object.entries(stoneData)) {
            const stoneId = stoneIdsMap.get(stoneName);
            const { SKU, SupplierPrice, SupplierShowcasePrice, ...otherData } =
              variation;

            currentSKUs.add(SKU);

            if (existingVariationsMap.has(SKU)) {
              const existingData = existingVariationsMap.get(SKU);

              if (
                existingData.supplier_price !== SupplierPrice ||
                existingData.supplier_showcase_price !== SupplierShowcasePrice
              ) {
                console.log(
                  "existingData.supplier_price",
                  existingData.supplier_price
                );

                console.log("supplier_price", SupplierPrice);

                console.log(
                  "existingData.supplier_showcase_price",
                  existingData.supplier_showcase_price
                );

                console.log("SupplierShowcasePrice", SupplierShowcasePrice);

                variationsToUpdate.push([
                  SupplierPrice,
                  SupplierShowcasePrice,
                  existingData.variation_id,
                ]);

                // ✅ Add to Shopify update list
                if (skuSyncSet.has(SKU)) {
                  shopifyVariationsToUpdate.push({
                    id: existingData.sync_id,
                    variantId: existingData.variant_sync_id,
                    price: SupplierShowcasePrice,
                  });
                }
              }
            } else {
              variationsToInsert.push([
                ringId,
                metalId,
                stoneId,
                variation.Title,
                SKU,
                variation.Stuller_p_id,
                variation.Description,
                variation.GroupDescription,
                variation.Status,
                SupplierPrice,
                SupplierShowcasePrice,
                variation.GramWeight,
                variation.RingSize,
                variation.LeadTime,
                variation.OnHand,
                variation.Orderable,
                variation.CurrencyCode,
                variation.Width,
                variation.PrimaryStoneType,
                variation.Quality,
                JSON.stringify(variation.SetWith || []),
                variation.Sync,
                variation.SyncId,
                variation.VariantSyncId,
              ]);
              insertVariationSKU.add(SKU);
            }
          }
        }
      }

      // ✅ Insert new variations
      if (variationsToInsert.length > 0) {
        console.log("Inserting new variations:", variationsToInsert.length);
        await connection.query(
          `INSERT INTO stlr_ring_variations 
                    (ring_id, metal_id, stone_id, title, sku, stuller_p_id, description, group_description, status, 
                    supplier_price, supplier_showcase_price, weight, ring_size, lead_time, onhand, orderable, 
                    currency_code, band_width, stone_type, quality, set_with, sync, sync_id, variant_sync_id) 
                    VALUES ?`,
          [variationsToInsert]
        );

        // ✅ Fetch inserted variations to update mapping
        const [variationRows] = await connection.query(
          "SELECT variation_id, sku FROM stlr_ring_variations WHERE sku IN (?)",
          [[...insertVariationSKU]]
        );
        variationRows.forEach((row) =>
          skuToVariationMap.set(row.sku, row.variation_id)
        );
      }

      // ✅ Batch update existing variations
      if (variationsToUpdate.length > 0) {
        console.log("Updating existing variations:", variationsToUpdate.length);
        const BATCH_SIZE = 100;

        for (let i = 0; i < variationsToUpdate.length; i += BATCH_SIZE) {
          const batch = variationsToUpdate.slice(i, i + BATCH_SIZE);

          const caseSupplierPrice = batch
            .map(([price, showcasePrice, id]) => `WHEN ${id} THEN ${price}`)
            .join(" ");
          const caseShowcasePrice = batch
            .map(
              ([price, showcasePrice, id]) => `WHEN ${id} THEN ${showcasePrice}`
            )
            .join(" ");
          const variationIds = batch.map(([_, __, id]) => id).join(", ");

          const updateQuery = `
                    UPDATE stlr_ring_variations 
                    SET 
                        supplier_price = CASE variation_id ${caseSupplierPrice} END,
                        supplier_showcase_price = CASE variation_id ${caseShowcasePrice} END
                    WHERE variation_id IN (${variationIds}); `;

          await connection.query(updateQuery);
        }
      }

      // ✅ Find variations to delete
      const existingSKUs = new Set(existingVariationsMap.keys());
      const skusToDelete = [...existingSKUs].filter(
        (sku) => !currentSKUs.has(sku)
      );

      if (skusToDelete.length > 0) {
        console.log("IdsToDelete");
        // Get variation IDs for deletion
        // const variationIdsToDelete = skusToDelete.map(sku => existingVariationsMap.get(sku).variation_id);
        // const ringIdsToCheck = new Set(skusToDelete.map(sku => existingVariationsMap.get(sku).ring_id));

        const variationIdsToDelete = [];
        const ringIdsToCheck = new Set();

        for (const sku of skusToDelete) {
          const variation = existingVariationsMap.get(sku);
          if (variation) {
            variationIdsToDelete.push(variation.variation_id);
            ringIdsToCheck.add(variation.ring_id);

            // Check if the SKU exists in skuSyncSet before adding sync_id
            if (skuSyncSet.has(sku) && variation.sync_id) {
              syncIdsToDelete.add(variation.sync_id);
            }
          }
        }

        console.log(syncIdsToDelete); // ✅ Set of unique sync_id values

        if (variationIdsToDelete.length > 0) {
          console.log(
            "variationIdsToDelete.length",
            variationIdsToDelete.length
          );
          console.log(
            "variationIdsToDelete",
            JSON.stringify(variationIdsToDelete)
          );

          // ✅ Delete variations safely
          await connection.query(
            "DELETE FROM stlr_ring_variations WHERE variation_id IN (?)",
            [variationIdsToDelete]
          );
        }

        // ✅ Check if rings should be deleted
        for (const ringId of ringIdsToCheck) {
          const [[{ count }]] = await connection.query(
            "SELECT COUNT(*) as count FROM stlr_ring_variations WHERE ring_id = ?",
            [ringId]
          );

          if (count === 0) {
            // ✅ Check if the ring exists in stlr_ring_has_categories
            const [[{ categoryCount }]] = await connection.query(
              "SELECT COUNT(*) as categoryCount FROM stlr_ring_has_categories WHERE ring_id = ?",
              [ringId]
            );

            if (categoryCount > 0) {
              // ✅ Delete from stlr_ring_has_categories first
              await connection.query(
                "DELETE FROM stlr_ring_has_categories WHERE ring_id = ?",
                [ringId]
              );
            }

            // ✅ Now delete from stlr_rings
            await connection.query("DELETE FROM stlr_rings WHERE ring_id = ?", [
              ringId,
            ]);
          }
        }
      }

      // ✅ Update Shopify Products
      // console.log("V333");
      console.log("shopifyVariationsToUpdate is", shopifyVariationsToUpdate);

      if (shopifyVariationsToUpdate.length > 0) {
        console.log("v1", shopifyVariationsToUpdate.length);
        const PRODUCT_BULK_UPDATE = `
                        mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
                            productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                                userErrors {
                                field
                                message
                                }
                            }
                        }`;

        for (let i = 0; i < shopifyVariationsToUpdate.length; i++) {
          const { id, variantId, price } = shopifyVariationsToUpdate[i];

          const productUpdateResponse = await graphqlClient(
            PRODUCT_BULK_UPDATE,
            {
              productId: id,
              variants: [
                {
                  id: variantId,
                  price: price,
                },
              ],
            }
          );

          const errors =
            productUpdateResponse?.data?.productVariantsBulkUpdate?.userErrors;
          if (errors && errors.length > 0) {
            console.error(
              `GraphQL Update Error for variant ${variantId}:`,
              errors
            );

            // ❌ Rollback and send failure response
            await connection.rollback();
            connection.release();
            return res.status(500).json({
              status: 500,
              success: false,
              message: "Shopify variant update failed",
              errors,
            });
          }

          console.log(`✅ Variant ${variantId} updated successfully.`);
        }
      }

      // ✅ Delete Shopify Products
      console.log("syncIdsToDelete is", syncIdsToDelete);

      if (syncIdsToDelete.size > 0) {
        console.log("v2", syncIdsToDelete.size);
        const PRODUCT_DELETE = `
                        mutation productDelete($input: ProductDeleteInput!) {
                            productDelete(input: $input) {
                                userErrors {
                                field
                                message
                                }
                            }
                        }`;

        const productIds = Array.from(syncIdsToDelete);
        for (let i = 0; i < productIds.length; i++) {
          const productDeleteResponse = await graphqlClient(PRODUCT_DELETE, {
            input: {
              id: productIds[i],
            },
          });

          const errors = productDeleteResponse?.data?.productDelete?.userErrors;
          if (errors && errors.length > 0) {
            console.error(
              `GraphQL Delete Error for product ${productIds[i]}:`,
              errors
            );

            // ❌ Rollback and send failure response
            await connection.rollback();
            connection.release();
            return res.status(500).json({
              status: 500,
              success: false,
              message: "Shopify product delete failed",
              errors,
            });
          }

          console.log(`✅ Product ${productIds[i]} deleted successfully.`);
          console.log(
            "productDeleteResponse",
            JSON.stringify(productDeleteResponse)
          );
        }
      }

      await connection.commit(); // ✅ Commit transaction
      console.log("Transaction committed successfully");
      return res.status(200).json({
        status: 200,
        success: true,
        message: "Completed successfully",
      });
    } catch (error) {
      await connection.rollback(); // ❌ Rollback if error occurs
      console.error("Transaction rolled back due to error:", error);
      return res.status(500).json({
        status: 500,
        success: false,
        message: "Internal server error",
        error: error.message || error,
      });
    } finally {
      connection.release(); // ✅ Release connection back to the pool
    }
  };

  // await insertData(transformedData);
  await insertOrUpdateData(transformedData);
  // await insertData(transformedData[6])
  console.log("transformedData", transformedData.length);
});

export { insertStullerInDb };
