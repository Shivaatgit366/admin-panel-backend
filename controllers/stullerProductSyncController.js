import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import Joi from "joi";
import { httpStatusCodes } from "./errorController.js";
import { sendEmail } from "../services/ses.js";
import axios from "axios";

const CREATE_PRODUCT_WITH_MEDIA = `
mutation CreateProductWithNewMedia($input: ProductInput!, $media: [CreateMediaInput!]) {
  productCreate(input: $input, media: $media) {
    product {
        id
        title
        variants(first: 1) {
            nodes {
                inventoryItem {
                id
                }
                id
            }
        }
    }
    userErrors {
      field
      message
    }
  }
}
`;

const CREATE_PRODUCT = `
mutation CreateProduct($input: ProductInput!) {
  productCreate(input: $input) {
    product {
      id
      title
      variants(first: 1) {
        nodes {
          inventoryItem {
            id
          }
          id
        }
      }
    }
    userErrors {
      field
      message
    }
  }
}
`;

const INVENTORY_ADJUST_QUANTITIES = `
      mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
        inventoryAdjustQuantities(input: $input) {
          userErrors {
            field
            message
          }
          inventoryAdjustmentGroup {
            createdAt
            reason
            referenceDocumentUri
            changes {
              name
              delta
            }
          }
        }
      }`;

const PUBLISHABLE_PUBLISH = `
      mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
        publishablePublish(id: $id, input: $input) {
          publishable {
            availablePublicationsCount {
              count
            }
          }
          userErrors {
            field
            message
          }
        }
      }`;

const PUBLISHABLE_IDS_QUERY = `
    query {
      publications(first: 100) {
        nodes {
          id
          name
        }
      }
    }
  `;

const PRODUCT_BULK_UPDATE = `
      mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    product {
      id
    }
    productVariants {
      id
      metafields(first: 2) {
        edges {
          node {
            namespace
            key
            value
          }
        }
      }
    }
    userErrors {
      field
      message
    }
  }
}`;
const COLLECTION_CHECK_QUERY = `
query CheckCollection($id: ID!) {
    collection(id: $id) { id title }
}`;

const DELETE_PRODUCT = `
  mutation productDelete($id: ID!) {
    productDelete(input: {id: $id}) {
      deletedProductId
      userErrors {
        field
        message
      }
    }
  }
`;

const PRODUCT_UPDATE = `
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      title
    }
    userErrors {
      field
      message
    }
  }
}`;

const GET_PRODUCT_QUERY = `
query GetProduct($id: ID!) {
    product(id: $id) {
        id
        title
        metafields(first: 100) {
          edges {
            node {
              id
              key
            }
          }
        }
    }
}
`;

const GET_METAFIELDS_QUERY = `
        query ProductMetafields($ownerId: ID!) {
            product(id: $ownerId) {
                metafields(first: 50) {
                    edges {
                        node {
                            id
                            key
                        }
                    }
                }
            }
        }`;

const syncProduct = async (req, res) => {
  let productId = null,
    variantId = null,
    temp_variation_id = null;
  let sqlUpdated = false;
  let checkError = false;

  try {
    const variantSchema = Joi.object({
      variation_id: Joi.number().required(),
    });
    const { error } = variantSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { variation_id } = req.body;
    temp_variation_id = variation_id;
    // Fetch paginated groups
    const query = `
                SELECT 
                    rv.*, 
                    JSON_OBJECT('metal_id', m.metal_id, 'metal_name', m.name) AS metal,
                    JSON_OBJECT('stone_id', s.stone_id, 'stone_name', s.name) AS stone,
                    JSON_OBJECT(
                        'ring_id', r.ring_id,
                        'supplier_group_id', r.supplier_group_id,
                        'group_id', r.group_id,
                        'group_name', g.group_name,
                        'category_id', r.category_id,
                        'category_name', c.name,
                        'category_shopify_id', c.shopify_id,
                        'style_id', st.style_id,
                        'style_name', st.style_name,
                        'gender_id', sg.gender_id,
                        'gender_type', sg.type
                    ) AS ring,
                    JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'web_cat_id', wc.web_cat_id,
                            'name', wc.name,
                            'path', wc.path,
                            'image_url', wc.image_url
                        )
                    ) AS webcategories
                FROM stlr_ring_variations rv
                LEFT JOIN stlr_rings r ON rv.ring_id = r.ring_id
                LEFT JOIN stlr_styles st ON r.style_id = st.style_id
                LEFT JOIN stlr_genders sg ON r.gender_id = sg.gender_id
                LEFT JOIN stlr_groups g ON r.group_id = g.group_id
                LEFT JOIN stlr_categories c ON r.category_id = c.category_id
                LEFT JOIN stlr_ring_has_categories rhc ON r.ring_id = rhc.ring_id
                LEFT JOIN stlr_webcategories wc ON rhc.web_cat_id = wc.web_cat_id
                LEFT JOIN stlr_metals m ON rv.metal_id = m.metal_id
                LEFT JOIN stlr_stones s ON rv.stone_id = s.stone_id
                WHERE rv.variation_id = ?
                GROUP BY rv.variation_id, m.metal_id, s.stone_id, r.ring_id, g.group_id, g.group_name, c.category_id, c.name, st.style_id, sg.gender_id;
    `;
    const [data] = await sqldb.query(query, variation_id);
    const product = data[0];
    console.log("product is", product);

    if (!product || product.length === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "No data found",
      });
    }

    // if the product is deactivated/archived, then make it to "active"
    if (!product.sync && product.sync_id && product.variant_sync_id) {
      const activateProduct = await graphqlClient(PRODUCT_UPDATE, {
        input: {
          id: product.sync_id,
          status: "ACTIVE",
        },
      });

      if (!activateProduct.success) {
        return res.status(500).json({
          status: httpStatusCodes.INTERNAL_SERVER,
          success: false,
          message: "Failed to activate product on Shopify",
        });
      }

      // Update database sync status
      const updatequery = `
        UPDATE stlr_ring_variations  
        SET sync = ?  
        WHERE variation_id = ?;
        `;
      await sqldb.execute(updatequery, [1, variation_id]);

      return res.status(200).json({
        status: httpStatusCodes.OK,
        success: true,
        message: "Product activated successfully",
      });
    }

    if (!product.title || !product.description || !product.band_width) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Title, description and band width are required",
      });
    }

    if (product.stone_type !== "NS" && product.diamonds == null) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Diamonds all fields are required",
      });
    }

    // Verify Shopify collection
    const checkCollection = await graphqlClient(COLLECTION_CHECK_QUERY, {
      id: product.ring.category_shopify_id,
    });

    if (!checkCollection.success || !checkCollection.data.collection) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Shopify collection does not exist.",
      });
    }
    console.log("checkCollection", JSON.stringify(checkCollection));

    // Prepare Shopify product data
    const productInput = {
      title: product.title,
      descriptionHtml: `<p>${product.description}</p>`,
      vendor: "Stuller",
      productType: product.group_description || "Jewelry",
      status: "ACTIVE",
      seo: {
        title: product.title,
        description: product.description,
      },
      metafields: [
        {
          namespace: "custom",
          key: "group_name",
          value: product.ring.group_name,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "band_width",
          value: product.band_width,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "stone_type",
          value: product.stone_type,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "diamonds",
          value: JSON.stringify(product.diamonds),
          type: "json",
        },
        {
          namespace: "custom",
          key: "stuller_p_id",
          value: product.stuller_p_id,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "metal",
          value: product.metal.metal_name,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "style",
          value: product.ring.style_name,
          type: "single_line_text_field",
        },
        {
          namespace: "custom",
          key: "gender",
          value: product.ring.gender_type,
          type: "single_line_text_field",
        },
      ],
      collectionsToJoin: [product.ring.category_shopify_id],
      // productOptions: [
      //   { name: "Metal", values: [{ name: product.quality }] },
      //   ...(product.stone_type !== "NS"
      //     ? [{ name: "Stone Type", values: [{ name: product.stone_type }] }]
      //     : []),
      // ],
    };

    // Create Shopify product
    const createProductResponse = await graphqlClient(CREATE_PRODUCT, {
      input: productInput,
    });

    if (
      !createProductResponse.success ||
      createProductResponse.data.productCreate.userErrors.length > 0
    ) {
      return res.status(500).json({
        status: httpStatusCodes.INTERNAL_SERVER,
        success: false,
        message: `Failed to create product on Shopify: ${createProductResponse.error}`,
      });
    }

    productId = createProductResponse?.data?.productCreate?.product?.id;
    variantId =
      createProductResponse?.data?.productCreate?.product?.variants?.nodes?.[0]
        ?.id;
    const inventoryItemId =
      createProductResponse?.data?.productCreate?.product?.variants?.nodes?.[0]
        ?.inventoryItem?.id;
    console.log("createProductResponse", JSON.stringify(createProductResponse));

    // Add value to the "Product Description" metafield
    const productDescriptionMetafield = {
      namespace: "productdata",
      key: "product_description",
      type: "json",
      value: JSON.stringify({
        "Ring Information": removeEmptyOrZeroFields({
          Style: product.styleId,
          Metal: product.metal.metal_name,
          Width: product.band_width,
        }),
        ...(product.diamonds &&
          Object.keys(formatDiamonds(product.diamonds)).length > 0 && {
            "Accent Gemstones": formatDiamonds(product.diamonds),
          }),
      }),
    };

    const addDescriptionMetafieldRes = await graphqlClient(PRODUCT_UPDATE, {
      input: {
        id: productId,
        metafields: [productDescriptionMetafield],
      },
    });

    if (!addDescriptionMetafieldRes.success) {
      checkError = true;
    }

    // Get the location ID for inventory adjustment
    const GET_LOCATIONS = `
      query {
        locations(first: 1) {
          nodes {
            id
            name
          }
        }
      }
    `;

    // 2. Run the query to get a valid locationId
    const locationResponse = await graphqlClient(GET_LOCATIONS);
    const locationId = locationResponse?.data?.locations?.nodes?.[0]?.id;

    if (!locationId) {
      console.error("No valid location found.");
      checkError = true;
    } else {
      // Adjust inventory
      const addInventory = await graphqlClient(INVENTORY_ADJUST_QUANTITIES, {
        input: {
          reason: "correction",
          name: "available",
          changes: [
            {
              delta: 100,
              inventoryItemId: inventoryItemId,
              locationId: locationId,
            },
          ],
        },
      });

      if (
        !addInventory.success ||
        addInventory.data.inventoryAdjustQuantities.userErrors.length > 0
      )
        checkError = true;
      console.log("addInventory", JSON.stringify(addInventory));
    }

    // Update variant details
    const productUpdateResponse = await graphqlClient(PRODUCT_BULK_UPDATE, {
      productId: productId,
      variants: [
        {
          id: variantId,
          // optionValues: [
          //   { optionName: "Metal", name: product.quality },
          //   ...(product.stone_type !== "NS"
          //     ? [{ optionName: "Stone Type", name: product.stone_type }]
          //     : []),
          // ],
          inventoryItem: {
            sku: product.sku,
            tracked: true,
            measurement: {
              weight: {
                value: parseFloat(product.weight),
                unit: "GRAMS",
              },
            },
          },
          inventoryPolicy: "DENY",
          price: product.supplier_showcase_price,
        },
      ],
    });

    if (
      !productUpdateResponse.success ||
      productUpdateResponse.data.productVariantsBulkUpdate.userErrors.length > 0
    )
      checkError = true;
    console.log("productUpdateResponse", JSON.stringify(productUpdateResponse));

    const getAllPublishableIds = await graphqlClient(PUBLISHABLE_IDS_QUERY);
    if (
      !getAllPublishableIds.success ||
      getAllPublishableIds.data.publications.nodes.length === 0
    )
      checkError = true;

    const publicationInputs = getAllPublishableIds.data.publications.nodes.map(
      (pub) => ({
        publicationId: pub.id,
      })
    );
    console.log("publicationInputs1", JSON.stringify(publicationInputs));
    // Publish product
    const publishtResponse = await graphqlClient(PUBLISHABLE_PUBLISH, {
      id: productId,
      input: publicationInputs,
    });
    if (
      !publishtResponse.success ||
      publishtResponse.data.publishablePublish.userErrors.length > 0
    )
      checkError = true;
    console.log("publishtResponse", JSON.stringify(publishtResponse));

    // -------------------------------------
    // Define the quality order
    const qualityOrder = [
      "14K Yellow Gold",
      "18K Yellow Gold",
      "14K White Gold",
      "18K White Gold",
      "14K Rose Gold",
      "18K Rose Gold",
      "Platinum",
    ];

    // Fetch all variations for the given ring_id
    const [variationData] = await sqldb.query(
      `SELECT sync_id, quality, variation_id 
      FROM stlr_ring_variations 
      WHERE ring_id = ? AND sync_id IS NOT NULL AND sync_id <> '';`,
      [product.ring_id]
    );

    // Append the current product variation
    const variations = [
      ...variationData,
      {
        sync_id: productId,
        quality: product.quality,
        variation_id: product.variation_id,
      },
    ];

    // Sort variations by quality order
    variations.sort(
      (a, b) =>
        qualityOrder.indexOf(a.quality) - qualityOrder.indexOf(b.quality)
    );

    // Extract sorted sync_ids
    const syncIds = variations.map((v) => v.sync_id);

    // Process each product sequentially
    for (const syncId of syncIds) {
      // Fetch metafields for the product
      const metaRes = await graphqlClient(GET_METAFIELDS_QUERY, {
        ownerId: syncId,
      });
      if (!metaRes.success) checkError = true;

      const metaData = metaRes?.data?.product?.metafields?.edges || [];
      const metaId = metaData.find((edge) => edge.node.key === "product_group")
        ?.node.id;

      const metafieldUpdate = metaId
        ? { id: metaId, value: JSON.stringify(syncIds) } // Update existing metafield
        : {
            namespace: "custom",
            key: "product_group",
            value: JSON.stringify(syncIds),
            type: "list.product_reference",
          }; // Create new metafield

      // Update the metafield for the product
      const updateProductMetaRes = await graphqlClient(PRODUCT_UPDATE, {
        input: { id: syncId, metafields: [metafieldUpdate] },
      });

      if (!updateProductMetaRes.success) checkError = true;
    }

    // -------------------------------------

    // Check for errors
    if (checkError) {
      // **Rollback if Shopify Sync Fails**
      if (productId) {
        await graphqlClient(DELETE_PRODUCT, { id: productId });
        console.log(`Rolled back product: ${productId}`);
      }
      return res.status(500).json({
        status: httpStatusCodes.INTERNAL_SERVER,
        success: false,
        message: "Failed to sync product on Shopify",
      });
    }
    // Update database sync status
    const updatequery = `
        UPDATE stlr_ring_variations  
        SET sync = ?, sync_id = ?, variant_sync_id = ?  
        WHERE variation_id = ?;
        `;
    const updateres = await sqldb.execute(updatequery, [
      1,
      productId,
      variantId,
      variation_id,
    ]);
    sqlUpdated = true;

    console.log("Product synced successfully", JSON.stringify(updateres));

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Product synced successfully",
      input: product,
      data: {
        productId,
        variantId,
        inventoryItemId,
      },
    });
  } catch (error) {
    // **Rollback if Shopify Sync Fails**
    if (productId) {
      await graphqlClient(DELETE_PRODUCT, { id: productId });
      console.log(`Rolled back product: ${productId}`);
    }

    // **Revert SQL Update if Already Applied**
    if (sqlUpdated) {
      await sqldb.execute(
        `UPDATE stlr_ring_variations SET sync = ?, sync_id = ?, variant_sync_id = ? WHERE variation_id = ?`,
        [0, null, null, temp_variation_id]
      );
    }

    console.error("Error fetching groups:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const asyncProduct = async (req, res) => {
  try {
    const variantSchema = Joi.object({
      variation_id: Joi.number().required(),
      sync_id: Joi.string().required(),
    });
    const { error } = variantSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { variation_id, sync_id } = req.body;

    // Check if variation_id and sync_id are associated in the database
    const checkQuery = `
    SELECT COUNT(*) AS count 
    FROM stlr_ring_variations 
    WHERE variation_id = ? AND sync_id = ?;
  `;
    const [rows] = await sqldb.query(checkQuery, [variation_id, sync_id]);

    if (rows[0].count === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Variation ID and sync_id not found or not associated",
      });
    }
    //deactivate product
    const deActivateProduct = await graphqlClient(PRODUCT_UPDATE, {
      input: {
        id: sync_id,
        status: "ARCHIVED",
      },
    });

    if (!deActivateProduct.success) {
      return res.status(500).json({
        status: httpStatusCodes.INTERNAL_SERVER,
        success: false,
        message: "Failed to deactivate product on Shopify",
      });
    }
    // Update database sync status
    const updatequery = `
        UPDATE stlr_ring_variations  
        SET sync = ? WHERE variation_id = ?;
        `;

    await sqldb.execute(updatequery, [0, variation_id]);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Product deactivated and sync status updated",
    });
  } catch (error) {
    console.error("Error fetching groups:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const deleteShopifyProduct = async (req, res) => {
  try {
    const variantSchema = Joi.object({
      variation_id: Joi.number().required(),
      sync_id: Joi.string().required(),
    });
    const { error } = variantSchema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { variation_id, sync_id } = req.body;

    // Check if variation_id and sync_id are associated in the database
    const checkQuery = `
    SELECT COUNT(*) AS count 
    FROM stlr_ring_variations 
    WHERE variation_id = ? AND sync_id = ?;
  `;
    const [rows] = await sqldb.query(checkQuery, [variation_id, sync_id]);

    if (rows[0].count === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Variation ID and sync_id not found or not associated",
      });
    }
    //delete product from shopify
    const deleteProduct = await graphqlClient(DELETE_PRODUCT, {
      id: sync_id,
    });
    if (!deleteProduct.success) {
      return res.status(500).json({
        status: httpStatusCodes.INTERNAL_SERVER,
        success: false,
        message: "Failed to delete product on Shopify",
      });
    }

    // Delete the product/variant from the MySQL DB
    const deleteQuery = `
          DELETE FROM stlr_ring_variations  
          WHERE variation_id = ?;
        `;

    await sqldb.execute(deleteQuery, [variation_id]);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Product deleted from Shopify and removed from database also",
    });
  } catch (error) {
    console.error("Error fetching groups:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const editProduct = async (req, res) => {
  const {
    variation_id,
    title,
    description,
    band_width,
    diamonds,
    styleId,
    ring_id,
  } = req.body;

  try {
    // Step 1: Fetch existing variation
    const [rows] = await sqldb.query(
      `
      SELECT 
        rv.*, 
        m.name AS metal_name 
      FROM 
        stlr_ring_variations AS rv
      LEFT JOIN 
        stlr_metals AS m 
        ON rv.metal_id = m.metal_id
      WHERE 
        rv.variation_id = ?
      `,
      [variation_id]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Variation ID not found",
      });
    }

    const { stone_type, sync_id, metal_name } = rows[0];

    // Step 2: Validate input
    const schemaFields = {
      variation_id: Joi.number().required(),
      title: Joi.string().required(),
      description: Joi.string().required(),
      band_width: Joi.string().required(),
      styleId: Joi.string().allow("", null).optional(),
      ring_id: Joi.number().required().messages({
        "any.required": "ring_id is required.",
        "number.base": "ring_id must be a number.",
      }),
    };

    if (stone_type !== "NS") {
      schemaFields.diamonds = Joi.object()
        .pattern(
          Joi.string(), // key like diamond1, diamond2
          Joi.object({
            shape: Joi.any().required(),
            number: Joi.any().required(),
            min_carat_total_weight: Joi.any().required(),
            setting: Joi.string().allow("", null).required(),
            color: Joi.any().required(),
            clarity: Joi.any().required(),
          })
        )
        .required();
    }

    const { error } = Joi.object(schemaFields).validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    // Step 3: Check if ringId exists and is associated with a valid groupId
    const ringQuery = `
      SELECT r.*, g.group_name 
      FROM wedding_band.stlr_rings AS r
      INNER JOIN wedding_band.stlr_groups AS g 
      ON r.group_Id = g.group_Id
      WHERE r.ring_id = ?
        AND r.group_Id IS NOT NULL 
        AND r.group_Id != 0 
        AND r.group_Id != '';
    `;

    const [ringRows] = await sqldb.query(ringQuery, [ring_id]);

    if (ringRows.length === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Ring ID not found",
      });
    }

    // extract the group_name for the update
    const ringData = ringRows[0];
    const group_name = ringData.group_name;

    // Step 3: Use diamonds only if stone_type != "NS"
    const diamondsData = stone_type !== "NS" ? diamonds : null;

    // Step 4: Update on Shopify (if sync_id exists)
    if (sync_id) {
      const shopifyProduct = await graphqlClient(GET_PRODUCT_QUERY, {
        id: sync_id,
      });

      if (!shopifyProduct.success || !shopifyProduct.data.product) {
        return res.status(404).json({
          status: httpStatusCodes.NOT_FOUND,
          success: false,
          message: "Product not found on the Shopify",
        });
      }

      const existingMetafields =
        shopifyProduct.data.product.metafields.edges.reduce((acc, { node }) => {
          acc[node.key] = node.id;
          return acc;
        }, {});

      const metafieldsToUpdate = [];
      const addMetafield = (key, value, type) => {
        metafieldsToUpdate.push({
          ...(existingMetafields[key]
            ? { id: existingMetafields[key] }
            : { namespace: "custom", key, type }),
          value: type === "json" ? JSON.stringify(value) : value,
        });
      };

      // Prepare data for the update of metafields
      addMetafield("band_width", band_width, "single_line_text_field");
      if (stone_type !== "NS") {
        addMetafield("diamonds", diamondsData, "json");
      }
      addMetafield("group_name", group_name, "single_line_text_field");

      if (stone_type !== "NS") {
        // Add value to the "Product Description" metafield
        const productDescriptionMetafield = {
          namespace: "productdata",
          key: "product_description",
          type: "json",
          value: JSON.stringify({
            "Ring Information": removeEmptyOrZeroFields({
              Style: styleId,
              Metal: metal_name,
              Width: band_width,
            }),
            ...(diamondsData &&
              Object.keys(formatDiamonds(diamondsData)).length > 0 && {
                "Accent Gemstones": formatDiamonds(diamondsData),
              }),
          }),
        };

        metafieldsToUpdate.push(productDescriptionMetafield);
      }

      const updateRes = await graphqlClient(PRODUCT_UPDATE, {
        input: {
          id: sync_id,
          title,
          descriptionHtml: `<p>${description}</p>`,
          metafields: metafieldsToUpdate,
        },
      });

      if (!updateRes.success) {
        return res.status(500).json({
          status: httpStatusCodes.INTERNAL_SERVER,
          success: false,
          message: "Failed to update product on Shopify",
        });
      }
    }

    // Step 5: Update local DB
    const fields = [
      "title = ?",
      "description = ?",
      "band_width = ?",
      "ring_id = ?",
    ];
    const values = [title, description, band_width, ring_id];

    if (stone_type !== "NS") {
      fields.push("diamonds = ?");
      values.push(JSON.stringify(diamondsData));
    }

    if (typeof styleId !== "undefined" && styleId !== null) {
      fields.push("styleId = ?");
      values.push(styleId);
    }

    values.push(variation_id);
    await sqldb.execute(
      `UPDATE stlr_ring_variations SET ${fields.join(
        ", "
      )} WHERE variation_id = ?`,
      values
    );

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Product updated successfully",
    });
  } catch (error) {
    console.error("Error updating product:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const getOneProduct = async (req, res) => {
  // Validate request params
  const schema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Variation ID must be a number.",
      "number.integer": "Variation ID must be an integer.",
      "number.positive": "Variation ID must be a positive number.",
      "any.required": "Variation ID is required.",
    }),
  });

  const { error } = schema.validate(req.params);
  if (error) {
    return res.status(400).json({
      status: 400,
      success: false,
      message: error.details[0].message,
    });
  }

  const { id: variation_id } = req.params;

  // SQL query with joins for metals and stones
  const getProductQuery = `
  SELECT 
    v.*, 
    m.name AS metal,
    s.name AS stone,
    g.group_name AS \`group\`,
    c.name AS collection,
    st.style_name AS style,
    sg.type AS gender_type,
    CASE 
      WHEN 
        g.group_name IS NOT NULL AND g.group_name != '' AND
        c.name IS NOT NULL AND c.name != '' AND
        st.style_name IS NOT NULL AND st.style_name != '' AND
        sg.type IS NOT NULL AND sg.type != '' AND
        m.name IS NOT NULL AND m.name != '' AND
        s.name IS NOT NULL AND s.name != '' AND
        v.band_width IS NOT NULL AND v.band_width != '' AND
        v.title IS NOT NULL AND v.title != '' AND
        v.weight IS NOT NULL AND v.weight != ''
      THEN 'complete'
      ELSE 'incomplete'
    END AS productStatus
  FROM 
    wedding_band.stlr_ring_variations v
  LEFT JOIN 
    wedding_band.stlr_metals m ON v.metal_id = m.metal_id
  LEFT JOIN 
    wedding_band.stlr_stones s ON v.stone_id = s.stone_id
  LEFT JOIN 
    wedding_band.stlr_rings r ON v.ring_id = r.ring_id
  LEFT JOIN 
    wedding_band.stlr_groups g ON r.group_id = g.group_id
  LEFT JOIN 
    wedding_band.stlr_categories c ON r.category_id = c.category_id
  LEFT JOIN 
    wedding_band.stlr_styles st ON r.style_id = st.style_id
  LEFT JOIN
    wedding_band.stlr_genders sg ON r.gender_id = sg.gender_id
  WHERE 
    v.variation_id = ?
`;

  const connection = await sqldb.getConnection();
  try {
    const [rows] = await connection.execute(getProductQuery, [variation_id]);

    if (rows.length === 0) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "Product not found.",
      });
    }

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Product fetched successfully.",
      result: rows[0],
    });
  } catch (error) {
    console.error("Error fetching product:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const getAllProducts = async (req, res) => {
  try {
    // Allowed sort fields mapped to DB columns
    const sortFieldMap = {
      sku: "sku",
      title: "title",
      group_name: "`group`",
      style: "style",
      gender: "gender_type",
      stone: "stone",
      supplier_showcase_price: "supplier_showcase_price",
      updated_at: "updated_at",
    };

    // Joi Validation Schema
    const schema = Joi.object({
      page: Joi.number().integer().min(1).default(1),
      limit: Joi.number().integer().min(1).default(20),
      search: Joi.string().trim().allow("").optional(),
      sort_field: Joi.string()
        .valid(...Object.keys(sortFieldMap))
        .default("sku"),
      sort_order: Joi.string().valid("ASC", "DESC").default("DESC"),
      display: Joi.string()
        .valid("all", "synced", "archived", "yetToBeSynced")
        .default("all"),
    }).unknown(true);

    const { error, value } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { page, limit, search, sort_field, sort_order, display } = value;
    const offset = (page - 1) * limit;
    const searchQuery = search ? `%${search}%` : null;
    const orderByField = sortFieldMap[sort_field] || "v.title";

    // Sync condition based on `display`
    let syncCondition = "";
    if (display === "synced") {
      syncCondition = `AND v.sync = 1 AND v.sync_id IS NOT NULL AND v.sync_id <> ''`;
    } else if (display === "archived") {
      syncCondition = `AND v.sync = 0 AND v.sync_id IS NOT NULL AND v.sync_id <> ''`;
    } else if (display === "yetToBeSynced") {
      syncCondition = `AND v.sync = 0 AND (v.sync_id IS NULL OR v.sync_id = '')`;
    }

    // Main Data Query
    const dataQuery = `
        SELECT * FROM (
          SELECT 
            v.*, 
            m.name AS metal,
            s.name AS stone,
            g.group_name AS \`group\`,
            c.name AS collection,
            st.style_name AS style,
            sg.type AS gender_type,
            CASE 
              WHEN 
                (g.group_name IS NULL OR g.group_name = '' OR
                 c.name IS NULL OR c.name = '' OR
                 st.style_name IS NULL OR st.style_name = '' OR
                 sg.type IS NULL OR sg.type = '' OR
                 m.name IS NULL OR m.name = '' OR
                 s.name IS NULL OR s.name = '' OR
                 v.band_width IS NULL OR v.band_width = '' OR
                 v.title IS NULL OR v.title = '' OR
                 v.weight IS NULL OR v.weight = '') 
              THEN 'incomplete'
              ELSE 'complete'
            END AS productStatus,
            CASE 
              WHEN 
                m.name IS NOT NULL AND m.name <> '' AND
                s.name IS NOT NULL AND s.name <> '' AND
                g.group_name IS NOT NULL AND g.group_name <> '' AND
                c.name IS NOT NULL AND c.name <> '' AND
                st.style_name IS NOT NULL AND st.style_name <> '' AND
                sg.type IS NOT NULL AND sg.type <> ''
              THEN 'yes'
              ELSE 'no'
            END AS attributesAssigned
          FROM wedding_band.stlr_ring_variations v
          LEFT JOIN wedding_band.stlr_rings r ON v.ring_id = r.ring_id
          LEFT JOIN wedding_band.stlr_metals m ON v.metal_id = m.metal_id
          LEFT JOIN wedding_band.stlr_stones s ON v.stone_id = s.stone_id
          LEFT JOIN wedding_band.stlr_groups g ON r.group_id = g.group_id
          LEFT JOIN wedding_band.stlr_categories c ON r.category_id = c.category_id
          LEFT JOIN wedding_band.stlr_styles st ON r.style_id = st.style_id
          LEFT JOIN wedding_band.stlr_genders sg ON r.gender_id = sg.gender_id
          ${
            search
              ? "WHERE (v.title LIKE ? OR g.group_name LIKE ? OR v.stuller_p_id LIKE ?)"
              : ""
          }
          ${syncCondition}
        ) AS filtered_products
        WHERE filtered_products.attributesAssigned = 'yes'
        ORDER BY ${orderByField} ${sort_order}, filtered_products.updated_at DESC
        LIMIT ? OFFSET ?
      `;

    // If search is present, params: [searchQuery, searchQuery, limit, offset]
    // Else params: [limit, offset]
    const dataParams = search
      ? [searchQuery, searchQuery, searchQuery, limit, offset]
      : [limit, offset];

    const [products] = await sqldb.query(dataQuery, dataParams);

    /**
     * Get the summary of counts
     */
    // attribute condition
    const attributeCondition = `
      m.name IS NOT NULL AND m.name <> '' AND
      s.name IS NOT NULL AND s.name <> '' AND
      g.group_name IS NOT NULL AND g.group_name <> '' AND
      c.name IS NOT NULL AND c.name <> '' AND
      st.style_name IS NOT NULL AND st.style_name <> '' AND
      sg.type IS NOT NULL AND sg.type <> ''
      `;

    // Prepare search filter string; Prepare search values for counts;
    const searchCondition = search
      ? `AND (v.title LIKE ? OR g.group_name LIKE ? OR v.stuller_p_id LIKE ?)`
      : "";

    const countSearchParams = search
      ? [searchQuery, searchQuery, searchQuery]
      : [];

    // All products with attributesAssigned = 'yes'
    const [allCount] = await sqldb.query(
      `
      SELECT COUNT(*) AS count
      FROM wedding_band.stlr_ring_variations v
      LEFT JOIN wedding_band.stlr_rings r ON v.ring_id = r.ring_id
      LEFT JOIN wedding_band.stlr_metals m ON v.metal_id = m.metal_id
      LEFT JOIN wedding_band.stlr_stones s ON v.stone_id = s.stone_id
      LEFT JOIN wedding_band.stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN wedding_band.stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN wedding_band.stlr_styles st ON r.style_id = st.style_id
      LEFT JOIN wedding_band.stlr_genders sg ON r.gender_id = sg.gender_id
      WHERE ${attributeCondition} ${searchCondition}
      `,
      countSearchParams
    );

    const [syncedCount] = await sqldb.query(
      `
      SELECT COUNT(*) AS count
      FROM wedding_band.stlr_ring_variations v
      LEFT JOIN wedding_band.stlr_rings r ON v.ring_id = r.ring_id
      LEFT JOIN wedding_band.stlr_metals m ON v.metal_id = m.metal_id
      LEFT JOIN wedding_band.stlr_stones s ON v.stone_id = s.stone_id
      LEFT JOIN wedding_band.stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN wedding_band.stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN wedding_band.stlr_styles st ON r.style_id = st.style_id
      LEFT JOIN wedding_band.stlr_genders sg ON r.gender_id = sg.gender_id
      WHERE v.sync = 1 AND v.sync_id IS NOT NULL AND v.sync_id <> ''
        AND ${attributeCondition} ${searchCondition}
      `,
      countSearchParams
    );

    const [archivedCount] = await sqldb.query(
      `
      SELECT COUNT(*) AS count
      FROM wedding_band.stlr_ring_variations v
      LEFT JOIN wedding_band.stlr_rings r ON v.ring_id = r.ring_id
      LEFT JOIN wedding_band.stlr_metals m ON v.metal_id = m.metal_id
      LEFT JOIN wedding_band.stlr_stones s ON v.stone_id = s.stone_id
      LEFT JOIN wedding_band.stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN wedding_band.stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN wedding_band.stlr_styles st ON r.style_id = st.style_id
      LEFT JOIN wedding_band.stlr_genders sg ON r.gender_id = sg.gender_id
      WHERE v.sync = 0 AND v.sync_id IS NOT NULL AND v.sync_id <> ''
        AND ${attributeCondition} ${searchCondition}
      `,
      countSearchParams
    );

    const [yetToBeSyncedCount] = await sqldb.query(
      `
      SELECT COUNT(*) AS count
      FROM wedding_band.stlr_ring_variations v
      LEFT JOIN wedding_band.stlr_rings r ON v.ring_id = r.ring_id
      LEFT JOIN wedding_band.stlr_metals m ON v.metal_id = m.metal_id
      LEFT JOIN wedding_band.stlr_stones s ON v.stone_id = s.stone_id
      LEFT JOIN wedding_band.stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN wedding_band.stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN wedding_band.stlr_styles st ON r.style_id = st.style_id
      LEFT JOIN wedding_band.stlr_genders sg ON r.gender_id = sg.gender_id
      WHERE v.sync = 0 AND (v.sync_id IS NULL OR v.sync_id = '')
        AND ${attributeCondition} ${searchCondition}
      `,
      countSearchParams
    );

    // Define total
    let total = allCount[0].count;

    if (display === "synced") {
      total = syncedCount[0].count;
    } else if (display === "archived") {
      total = archivedCount[0].count;
    } else if (display === "yetToBeSynced") {
      total = yetToBeSyncedCount[0].count;
    }

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Products fetched successfully.",
      result: {
        products,
        page,
        limit,
        total_records: total,
        total_pages: Math.ceil(total / limit),
        counts: {
          allCount: allCount[0].count,
          syncedCount: syncedCount[0].count,
          archivedCount: archivedCount[0].count,
          yetToBeSyncedCount: yetToBeSyncedCount[0].count,
        },
      },
    });
  } catch (error) {
    console.error("Error fetching products:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const productActionsInBulk = async (req, res) => {
  try {
    const variantSchema = Joi.object({
      variation_ids: Joi.array()
        .items(Joi.number().required())
        .min(1)
        .required(),
      action: Joi.string().valid("sync", "unsync", "delete").required(),
    });

    const { error } = variantSchema.validate(req.body);

    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { variation_ids, action } = req.body;

    // Fetch full data with joins in one query
    const fetchQuery = `
      SELECT 
        rv.*, 
        JSON_OBJECT('metal_id', m.metal_id, 'metal_name', m.name) AS metal,
        JSON_OBJECT('stone_id', s.stone_id, 'stone_name', s.name) AS stone,
        JSON_OBJECT(
            'ring_id', r.ring_id,
            'supplier_group_id', r.supplier_group_id,
            'group_id', r.group_id,
            'group_name', g.group_name,
            'category_id', r.category_id,
            'category_name', c.name,
            'category_shopify_id', c.shopify_id,
            'style_id', st.style_id,
            'style_name', st.style_name,
            'gender_id', sg.gender_id,
            'gender_type', sg.type
        ) AS ring,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'web_cat_id', wc.web_cat_id,
                'name', wc.name,
                'path', wc.path,
                'image_url', wc.image_url
            )
        ) AS webcategories
      FROM stlr_ring_variations rv
      LEFT JOIN stlr_rings r ON rv.ring_id = r.ring_id
      LEFT JOIN stlr_styles st ON r.style_id = st.style_id
      LEFT JOIN stlr_genders sg ON r.gender_id = sg.gender_id
      LEFT JOIN stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN stlr_ring_has_categories rhc ON r.ring_id = rhc.ring_id
      LEFT JOIN stlr_webcategories wc ON rhc.web_cat_id = wc.web_cat_id
      LEFT JOIN stlr_metals m ON rv.metal_id = m.metal_id
      LEFT JOIN stlr_stones s ON rv.stone_id = s.stone_id
      WHERE rv.variation_id IN (?)
      GROUP BY rv.variation_id, m.metal_id, s.stone_id, r.ring_id, g.group_id, g.group_name, c.category_id, c.name, st.style_id, sg.gender_id;
    `;

    const [rows] = await sqldb.query(fetchQuery, [variation_ids]);

    if (rows.length === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "No variations found",
      });
    }

    const foundIds = rows.map((row) => row.variation_id);
    const notFoundIds = variation_ids.filter((id) => !foundIds.includes(id));

    if (notFoundIds.length > 0) {
      return res.status(httpStatusCodes.BAD_REQUEST).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Some variation ids ${notFoundIds.join(", ")} were not found`,
      });
    }

    // Create a map of variation_id -> stuller_p_id. This is done to show error messages to the client based on sku. Not based on variation ids because client can not see variation ids in browser.
    const variationToStullerMap = {};
    rows.forEach((row) => {
      variationToStullerMap[row.variation_id] = row.stuller_p_id;
    });

    // sync the products
    if (action === "sync") {
      /**
       * Validation Part
       */
      const alreadySynced = rows
        .filter((row) => row.sync === 1)
        .map((row) => variationToStullerMap[row.variation_id]);

      if (alreadySynced.length > 0) {
        return res.status(httpStatusCodes.BAD_REQUEST).json({
          status: httpStatusCodes.BAD_REQUEST,
          success: false,
          message: `Some products such as ${alreadySynced.join(
            ", "
          )} are already synced`,
        });
      }

      // validation to check for mandatory fields. Whether all the rows going for sync action have mandatory fields filled or not. If they dont have mandatory fields, then throw error.
      for (const row of rows) {
        const stullerId = variationToStullerMap[row.variation_id];

        if (!row.title || !row.description || !row.band_width) {
          return res.status(httpStatusCodes.BAD_REQUEST).json({
            status: httpStatusCodes.BAD_REQUEST,
            success: false,
            message: `Product with SKU ${stullerId} is missing title, description, or band width`,
          });
        }

        if (row.stone_type !== "NS" && row.diamonds == null) {
          return res.status(httpStatusCodes.BAD_REQUEST).json({
            status: httpStatusCodes.BAD_REQUEST,
            success: false,
            message: `Product with SKU ${stullerId} requires diamonds data`,
          });
        }
      }

      /**
       * Sync the products into the shopify
       */
      let successfullySynced = [];
      let failedToSync = [];

      for (const row of rows) {
        try {
          // Sync the product to Shopify and then return the shopifyProductId
          const { productId, variantId } = await syncSingleProduct(row);

          // If successful, track the variation_id or Shopify product ID
          successfullySynced.push({
            shopifyProductId: productId,
            variantId,
            variationId: row.variation_id,
          });
        } catch (syncError) {
          console.log("error is", syncError);
          failedToSync.push(row.variation_id);
        }
      }

      // Check if any products failed to sync; If there are any failed products, then rollback the entire process by deleting the successfully synced products from the shopify.
      if (failedToSync.length > 0) {
        for (const synced of successfullySynced) {
          try {
            // Delete each of those products from Shopify
            await deleteProductFromShopify(synced.shopifyProductId);
          } catch (deleteError) {
            // Log the failure to delete (do not throw — handle gracefully)
            console.log("error during rollback", deleteError);
          }
        }

        // Respond with an error indicating partial failure and rollback
        return res.status(httpStatusCodes.INTERNAL_SERVER).json({
          status: httpStatusCodes.INTERNAL_SERVER,
          success: false,
          message:
            "Sync failed for some products. All synced products were rolled back.",
        });
      }

      /**
       * Update the table
       */
      const connection = await sqldb.getConnection();
      await connection.beginTransaction();

      try {
        const updateSyncStatusQuery = `
          UPDATE stlr_ring_variations
          SET sync = 1, sync_id = ?, variant_sync_id = ?
          WHERE variation_id = ?
        `;

        for (const synced of successfullySynced) {
          await connection.query(updateSyncStatusQuery, [
            synced.shopifyProductId,
            synced.variantId,
            synced.variationId,
          ]);
        }

        await connection.commit();

        return res.status(200).json({
          status: httpStatusCodes.OK,
          success: true,
          message: `All selected products are synced`,
        });
      } catch (updateError) {
        await connection.rollback();

        // Rollback Shopify again if DB update fails
        for (const synced of successfullySynced) {
          try {
            await deleteProductFromShopify(synced.shopifyProductId);
          } catch (deleteError) {
            console.log("error during rollback", deleteError);
          }
        }

        return res.status(httpStatusCodes.INTERNAL_SERVER).json({
          status: httpStatusCodes.INTERNAL_SERVER,
          success: false,
          message:
            "Sync succeeded but DB update failed. So, the Changes are rolled back.",
        });
      } finally {
        connection.release();
      }
    }

    // unsync the products
    else if (action === "unsync") {
      /**
       * Validation Part
       */
      const notProperlySynced = rows
        .filter(
          (row) => !row.sync_id || row.sync_id.trim() === "" || row.sync !== 1
        )
        .map((row) => variationToStullerMap[row.variation_id]);

      if (notProperlySynced.length > 0) {
        return res.status(httpStatusCodes.BAD_REQUEST).json({
          status: httpStatusCodes.BAD_REQUEST,
          success: false,
          message: `Some products such as ${notProperlySynced.join(
            ", "
          )} are not yet synced`,
        });
      }

      /**
       * Unsync the products
       */
      let successfullyUnsynced = [];
      let failedToUnsync = [];

      for (const row of rows) {
        try {
          // archive the product
          await unsyncSingleProduct(row.sync_id);

          successfullyUnsynced.push({
            sync_id: row.sync_id,
            variation_id: row.variation_id,
          });
        } catch (unsyncError) {
          failedToUnsync.push(row.sync_id);
        }
      }

      // Check if any products failed to unsync; If there are any failed products, then rollback the entire process by putting back the successfully unsynced products into active products in shopify.
      if (failedToUnsync.length > 0) {
        for (const item of successfullyUnsynced) {
          try {
            // Re-sync back to Shopify (restore archived product)
            await syncArchivedProduct(item.sync_id);
          } catch (rollbackError) {
            // We log rollback failure gracefully, but don’t stop the process
            console.log(
              `Rollback failed for sync_id ${item.sync_id}`,
              rollbackError
            );
          }
        }

        // Inform the client that rollback has been performed
        return res.status(httpStatusCodes.INTERNAL_SERVER).json({
          status: httpStatusCodes.INTERNAL_SERVER,
          success: false,
          message:
            "Unsync action failed for some products. All unsynced products were rolled back.",
        });
      }

      /**
       * Update the table
       */
      const connection = await sqldb.getConnection();
      await connection.beginTransaction();

      try {
        const updateUnsyncStatusQuery = `
          UPDATE stlr_ring_variations
          SET sync = 0
          WHERE variation_id = ?
        `;

        for (const row of successfullyUnsynced) {
          await connection.query(updateUnsyncStatusQuery, [row.variation_id]);
        }

        await connection.commit();

        return res.status(200).json({
          status: httpStatusCodes.OK,
          success: true,
          message: `All selected products are un-synced or moved to archive`,
        });
      } catch (updateError) {
        await connection.rollback();

        // Try reactivating the products on Shopify again
        for (const row of successfullyUnsynced) {
          try {
            await syncArchivedProduct(row.sync_id);
          } catch (rollbackError) {
            console.log(
              `Rollback failed during DB error for sync_id ${row.sync_id}`,
              rollbackError
            );
          }
        }

        return res.status(httpStatusCodes.INTERNAL_SERVER).json({
          status: httpStatusCodes.INTERNAL_SERVER,
          success: false,
          message:
            "Unsync succeeded on Shopify but DB update failed. All changes were rolled back.",
        });
      } finally {
        connection.release();
      }
    }

    // delete the products
    else if (action === "delete") {
      /**
       * Validation Part
       */
      const invalidForDelete = rows
        .filter((row) => !row.sync_id || row.sync_id.trim() === "")
        .map((row) => variationToStullerMap[row.variation_id]);

      if (invalidForDelete.length > 0) {
        return res.status(httpStatusCodes.BAD_REQUEST).json({
          status: httpStatusCodes.BAD_REQUEST,
          success: false,
          message: `Some products such as ${invalidForDelete.join(
            ", "
          )} cannot be deleted as they are not even synced for the first time`,
        });
      }

      /**
       * Delete the products from shopify
       */
      let successfullyDeleted = [];
      let failedToDelete = [];

      for (const row of rows) {
        try {
          await deleteProductFromShopify(row.sync_id);

          successfullyDeleted.push(row.variation_id);
        } catch (deleteError) {
          failedToDelete.push(row.variation_id);
        }
      }

      /**
       * Update the table WITHOUT TRANSACTION/ATOMICITY CONCEPT. Because we are allowing the partial delete.
       */
      let deletedFromDB = [];
      let failedToDeleteFromDB = [];

      const deleteQuery = `
          DELETE FROM stlr_ring_variations  
          WHERE variation_id = ?;
        `;

      for (const variation_id of successfullyDeleted) {
        try {
          await sqldb.execute(deleteQuery, [variation_id]);
          deletedFromDB.push(variation_id);
        } catch (err) {
          failedToDeleteFromDB.push({ variation_id, error: err.message });
        }
      }

      // If there are failed items in shopify operation or in the db operation, then send the response with partially deleted items.
      if (failedToDelete.length > 0 || failedToDeleteFromDB.length > 0) {
        return res.status(httpStatusCodes.PARTIAL_CONTENT).json({
          status: httpStatusCodes.PARTIAL_CONTENT,
          success: false,
          message: "Some products failed to delete.",
          failedInShopify: failedToDelete.map(
            (id) => variationToStullerMap[id]
          ),
          failedInDB: failedToDeleteFromDB.map((item) => ({
            stullerProductId: variationToStullerMap[item.variation_id],
            error: item.error,
          })),
          deletedInShopify: successfullyDeleted.map(
            (id) => variationToStullerMap[id]
          ),
          deletedInDB: deletedFromDB.map((id) => variationToStullerMap[id]),
        });
      }

      // send response if all the delete operations are successfully completed.
      return res.status(200).json({
        status: httpStatusCodes.OK,
        success: true,
        message: "All selected products were successfully deleted.",
      });
    }
  } catch (error) {
    console.error("Error is:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const syncSingleProduct = async (row) => {
  try {
    const product = row;

    // If the product is already in shopify and the product is deactivated/archived, then make it to "active"
    if (!product.sync && product.sync_id && product.variant_sync_id) {
      const activateProduct = await graphqlClient(PRODUCT_UPDATE, {
        input: {
          id: product.sync_id,
          status: "ACTIVE",
        },
      });

      if (!activateProduct.success) {
        throw new Error("Failed to create product on Shopify");
      }

      return {
        productId: product.sync_id,
        variantId: product.variant_sync_id,
      };
    }

    let productId = null;
    let variantId = null;

    // 1. Create Product
    const createProductResponse = await graphqlClient(CREATE_PRODUCT, {
      input: {
        title: product.title,
        descriptionHtml: `<p>${product.description}</p>`,
        vendor: "Stuller",
        productType: product.group_description || "Jewelry",
        status: "ACTIVE",
        seo: {
          title: product.title,
          description: product.description,
        },
        metafields: [
          {
            namespace: "custom",
            key: "group_name",
            value: product.ring.group_name,
            type: "single_line_text_field",
          },
          // {
          //   namespace: "custom",
          //   key: "metal_name_with_purity",
          //   value: product.quality,
          //   type: "single_line_text_field",
          // },
          {
            namespace: "custom",
            key: "band_width",
            value: product.band_width,
            type: "single_line_text_field",
          },
          {
            namespace: "custom",
            key: "stone_type",
            value: product.stone_type,
            type: "single_line_text_field",
          },
          {
            namespace: "custom",
            key: "diamonds",
            value: JSON.stringify(product.diamonds),
            type: "json",
          },
          {
            namespace: "custom",
            key: "stuller_p_id",
            value: product.stuller_p_id,
            type: "single_line_text_field",
          },
          {
            namespace: "custom",
            key: "metal",
            value: product.metal.metal_name,
            type: "single_line_text_field",
          },
          {
            namespace: "custom",
            key: "style",
            value: product.ring.style_name,
            type: "single_line_text_field",
          },
          {
            namespace: "custom",
            key: "gender",
            value: product.ring.gender_type,
            type: "single_line_text_field",
          },
        ],
        collectionsToJoin: [product.ring.category_shopify_id],
        // productOptions: [
        //   { name: "Metal", values: [{ name: product.quality }] },
        //   ...(product.stone_type !== "NS"
        //     ? [{ name: "Stone Type", values: [{ name: product.stone_type }] }]
        //     : []),
        // ],
      },
    });

    if (
      !createProductResponse.success ||
      createProductResponse.data.productCreate.userErrors.length > 0
    ) {
      throw new Error("Failed to create product on Shopify");
    }

    productId = createProductResponse.data.productCreate.product.id;
    variantId =
      createProductResponse.data.productCreate.product.variants.nodes[0].id;
    const inventoryItemId =
      createProductResponse.data.productCreate.product.variants.nodes[0]
        .inventoryItem.id;

    // Add value to the "Product Description" metafield
    const productDescriptionMetafield = {
      namespace: "productdata",
      key: "product_description",
      type: "json",
      value: JSON.stringify({
        "Ring Information": removeEmptyOrZeroFields({
          Style: product.styleId,
          Metal: product.metal.metal_name,
          Width: product.band_width,
        }),
        ...(product.diamonds &&
          Object.keys(formatDiamonds(product.diamonds)).length > 0 && {
            "Accent Gemstones": formatDiamonds(product.diamonds),
          }),
      }),
    };

    const addDescriptionMetafieldRes = await graphqlClient(PRODUCT_UPDATE, {
      input: {
        id: productId,
        metafields: [productDescriptionMetafield],
      },
    });

    if (!addDescriptionMetafieldRes.success) {
      throw new Error("Failed to add product description metafield");
    }

    // Get the location ID for inventory adjustment
    const GET_LOCATIONS = `
      query {
        locations(first: 1) {
          nodes {
            id
            name
          }
        }
      }
    `;

    // 2. Run the query to get a valid locationId
    const locationResponse = await graphqlClient(GET_LOCATIONS);
    const locationId = locationResponse?.data?.locations?.nodes?.[0]?.id;

    if (!locationId) {
      throw new Error("No valid location found.");
    } else {
      // Adjust inventory
      const addInventory = await graphqlClient(INVENTORY_ADJUST_QUANTITIES, {
        input: {
          reason: "correction",
          name: "available",
          changes: [
            {
              delta: 100,
              inventoryItemId: inventoryItemId,
              locationId: locationId,
            },
          ],
        },
      });

      if (
        !addInventory.success ||
        addInventory.data.inventoryAdjustQuantities.userErrors.length > 0
      ) {
        throw new Error("Failed to adjust inventory");
      }
    }

    // 3. Update Variant
    const productUpdateResponse = await graphqlClient(PRODUCT_BULK_UPDATE, {
      productId: productId,
      variants: [
        {
          id: variantId,
          // optionValues: [
          //   { optionName: "Metal", name: product.quality },
          //   ...(product.stone_type !== "NS"
          //     ? [{ optionName: "Stone Type", name: product.stone_type }]
          //     : []),
          // ],
          inventoryItem: {
            sku: product.sku,
            tracked: true,
            measurement: {
              weight: {
                value: parseFloat(product.weight),
                unit: "GRAMS",
              },
            },
          },
          inventoryPolicy: "DENY",
          price: product.supplier_showcase_price,
        },
      ],
    });

    if (
      !productUpdateResponse.success ||
      productUpdateResponse.data.productVariantsBulkUpdate.userErrors.length > 0
    ) {
      throw new Error("Failed to update product variant");
    }

    // 4. Publish Product
    const getAllPublishableIds = await graphqlClient(PUBLISHABLE_IDS_QUERY);

    if (
      !getAllPublishableIds.success ||
      getAllPublishableIds.data.publications.nodes.length === 0
    ) {
      throw new Error("Failed to fetch publishable IDs");
    }

    const publicationInputs = getAllPublishableIds.data.publications.nodes.map(
      (pub) => ({ publicationId: pub.id })
    );

    const publishtResponse = await graphqlClient(PUBLISHABLE_PUBLISH, {
      id: productId,
      input: publicationInputs,
    });

    if (
      !publishtResponse.success ||
      publishtResponse.data.publishablePublish.userErrors.length > 0
    ) {
      throw new Error("Failed to publish product");
    }

    // 5. Handle Product Grouping (Metafields)
    const [variationData] = await sqldb.query(
      `SELECT sync_id, quality, variation_id 
     FROM stlr_ring_variations 
     WHERE ring_id = ? AND sync_id IS NOT NULL AND sync_id <> '';`,
      [product.ring_id]
    );

    const variations = [
      ...variationData,
      {
        sync_id: productId,
        quality: product.quality,
        variation_id: product.variation_id,
      },
    ];

    const qualityOrder = [
      "14K Yellow Gold",
      "18K Yellow Gold",
      "14K White Gold",
      "18K White Gold",
      "14K Rose Gold",
      "18K Rose Gold",
      "Platinum",
    ];

    variations.sort(
      (a, b) =>
        qualityOrder.indexOf(a.quality) - qualityOrder.indexOf(b.quality)
    );

    const syncIds = variations.map((v) => v.sync_id);

    for (const syncId of syncIds) {
      const metaRes = await graphqlClient(GET_METAFIELDS_QUERY, {
        ownerId: syncId,
      });

      if (!metaRes.success) {
        throw new Error(`Failed to fetch metafields for product ${syncId}`);
      }

      const metaId = metaRes.data.product.metafields.edges.find(
        (edge) => edge.node.key === "product_group"
      )?.node.id;

      const metafieldUpdate = metaId
        ? { id: metaId, value: JSON.stringify(syncIds) }
        : {
            namespace: "custom",
            key: "product_group",
            value: JSON.stringify(syncIds),
            type: "list.product_reference",
          };

      const updateProductMetaRes = await graphqlClient(PRODUCT_UPDATE, {
        input: { id: syncId, metafields: [metafieldUpdate] },
      });

      if (!updateProductMetaRes.success) {
        throw new Error(`Failed to update metafield for product ${syncId}`);
      }
    }

    return {
      productId,
      variantId,
    };
  } catch (error) {
    throw error;
  }
};

const deleteProductFromShopify = async (id) => {
  const deleteProduct = await graphqlClient(DELETE_PRODUCT, {
    id: id,
  });
  if (!deleteProduct.success) {
    throw new Error("Failed to delete product on Shopify");
  }
};

const unsyncSingleProduct = async (id) => {
  const deActivateProduct = await graphqlClient(PRODUCT_UPDATE, {
    input: {
      id: id,
      status: "ARCHIVED",
    },
  });

  if (!deActivateProduct.success) {
    throw new Error("Failed to deactivate/unsync product on Shopify");
  }
};

const syncArchivedProduct = async (id) => {
  const activateProduct = await graphqlClient(PRODUCT_UPDATE, {
    input: {
      id: id,
      status: "ACTIVE",
    },
  });

  if (!activateProduct.success) {
    throw new Error("Failed to activate product on Shopify");
  }
};

const getAllProductsCount = async (req, res) => {
  try {
    // Joi Validation Schema
    const schema = Joi.object({
      collectionId: Joi.string()
        .pattern(/^gid:\/\/shopify\/Collection\/\d+$/)
        .required()
        .messages({
          "string.base": `"collectionId" should be a type of 'text'`,
          "string.empty": `"collectionId" cannot be an empty field`,
          "any.required": `"collectionId" is a required field`,
          "string.pattern.base": `"collectionId" must be a valid Shopify GID (e.g., gid://shopify/Collection/1234567890)`,
        }),
      shape: Joi.string().trim().optional().allow(""),
      metal: Joi.string().trim().optional().allow(""),
      style: Joi.string().trim().optional().allow(""),
      gender: Joi.string().trim().optional().allow(""),
    }).unknown(true);

    const { error, value } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { collectionId, shape, metal, style, gender } = value;

    const filters = [];
    const metafieldQueriesList = [];

    if (shape && shape.trim() !== "") {
      metafieldQueriesList.push(
        `shape: metafield(namespace: "Custom", key: "Shape") { value }`
      );
      filters.push({ key: "shape", value: shape.trim() });
    }
    if (metal && metal.trim() !== "") {
      metafieldQueriesList.push(
        `metal: metafield(namespace: "Custom", key: "Metal") { value }`
      );
      filters.push({ key: "metal", value: metal.trim() });
    }
    if (style && style.trim() !== "") {
      metafieldQueriesList.push(
        `style: metafield(namespace: "Custom", key: "Style") { value }`
      );
      filters.push({ key: "style", value: style.trim() });
    }
    if (gender && gender.trim() !== "") {
      metafieldQueriesList.push(
        `gender: metafield(namespace: "Custom", key: "Gender") { value }`
      );
      filters.push({ key: "gender", value: gender.trim() });
    }

    const metafieldQueries = metafieldQueriesList.join("\n");

    const PRODUCT_QUERY = `
      query GetCollectionProducts($collectionId: ID!, $after: String) {
        collection(id: $collectionId) {
          products(first: 100, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              title
              status
              ${metafieldQueries}
            }
          }
        }
      }
    `;

    let hasNextPage = true;
    let endCursor = null;
    let matchingCount = 0;

    while (hasNextPage) {
      const result = await graphqlClient(PRODUCT_QUERY, {
        collectionId,
        after: endCursor,
      });

      if (!result.success) {
        throw new Error("Failed to fetch products from Shopify");
      }

      // Add this check:
      if (!result.data.collection) {
        return res.status(404).json({
          status: httpStatusCodes.NOT_FOUND,
          success: false,
          message: "Shopify collection not found for the given collectionId.",
        });
      }

      const { nodes, pageInfo } = result.data.collection.products;

      if (filters.length === 0) {
        matchingCount += nodes.filter((p) => p.status === "ACTIVE").length;
      } else {
        for (const product of nodes) {
          if (product.status !== "ACTIVE") continue;

          const isMatch = filters.every(
            ({ key, value }) => product[key]?.value === value
          );
          if (isMatch) matchingCount++;
        }
      }

      hasNextPage = pageInfo.hasNextPage;
      endCursor = pageInfo.endCursor;
    }

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Products count fetched successfully.",
      result: {
        productsCount: matchingCount,
      },
    });
  } catch (error) {
    console.error("Error fetching products:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const sendRegistrationEmail = async (req, res) => {
  const connection = await sqldb.getConnection();

  try {
    const schema = Joi.object({
      email: Joi.string().email().required(),
    });

    const { email } = req.body;
    const { error } = schema.validate({ email });

    if (error) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: "Please enter a valid email",
      });
    }

    // Check the user exists in the database
    const [rows] = await connection.execute(
      `SELECT * FROM customers WHERE email = ?`,
      [email]
    );

    if (rows.length > 0) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "Customer already registered with this email",
      });
    }

    // Registration link
    const registrationLinkWithEmail = `http://localhost:3000/account/activate?email=${email}`;

    // Send email using the imported SES service
    await sendEmail({
      to: email,
      subject: "Password Reset Link",
      html: `
        <p>Registration at wedding band</p>
        <p>Click the link below to Register</p>
        <a href="${registrationLinkWithEmail}">${registrationLinkWithEmail}</a>
      `,
    });

    return res.status(200).json({
      status: 200,
      success: true,
      message: "registration link sent successfully",
    });
  } catch (error) {
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    if (connection) connection.release();
  }
};

const updateEngagementProduct = async (req, res) => {
  try {
    // Joi Validation Schema
    const schema = Joi.object({
      diamond_id: Joi.string().required(),
      ring_id: Joi.string().required(),
    });

    const { error, value } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    let { diamond_id, ring_id } = value;

    diamond_id = diamond_id.replace(/[\u2010-\u2015]/g, "-");

    const url = new URL(
      "https://www.chicago-jewelers.com/api/getsinglediamonds"
    );
    url.searchParams.set("diamond_id", diamond_id);

    const response = await axios.get(url.toString());

    const { stone_price } = response.data;
    const stonePriceNum = parseFloat(stone_price);

    // Get the product which is present in the engagement rings collection
    const GET_PRODUCT_QUERY = `
      query GetProduct($id: ID!) {
        product(id: $id) {
          id
          title
          handle
          variants(first: 1) {
            edges {
              node {
                price 
              }
            }
          }
          collections(first: 10) {
            edges {
              node {
                handle
              }
            }
          }
        }
      }
    `;

    const { data, errors } = await graphqlClient(GET_PRODUCT_QUERY, {
      id: ring_id,
    });

    if (errors?.length || !data?.product) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Product not found on Shopify",
      });
    }

    const inEngagement = data.product.collections.edges.some(
      ({ node }) => node.handle === "engagement-rings"
    );

    if (!inEngagement) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Product not found in Engagement Rings collection",
      });
    }

    const product = data.product;
    const handle = product.handle;
    const price = product.variants?.edges?.[0]?.node?.price || null;

    const ringPriceNum = parseFloat(price);
    const diamondPrice = stonePriceNum;
    const totalPrice = ringPriceNum + diamondPrice;

    if (Number.isNaN(ringPriceNum) || Number.isNaN(diamondPrice)) {
      return res.status(500).json({
        status: httpStatusCodes.INTERNAL_SERVER,
        success: false,
        message: "Price parsing error",
      });
    }

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Engagement product updated.",
      data: {
        id: product.id,
        title: product.title,
        handle,
        price,
      },
    });
  } catch (error) {
    console.error("Error fetching products:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

export {
  syncProduct,
  asyncProduct,
  deleteShopifyProduct,
  editProduct,
  getOneProduct,
  getAllProducts,
  productActionsInBulk,
  getAllProductsCount,
  sendRegistrationEmail,
  updateEngagementProduct,
};

// Helper function to format keys
const formatKey = (key) => {
  return key.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
};

// Remove fields with empty string or 0 from an object
const removeEmptyOrZeroFields = (data) => {
  const cleaned = {};
  for (const [key, value] of Object.entries(data)) {
    if (value !== "" && value !== 0 && value !== null && value !== undefined) {
      cleaned[formatKey(key)] = value;
    }
  }
  return cleaned;
};

// Handle diamonds (Accent Gemstones)
const formatDiamonds = (diamonds) => {
  return Object.entries(diamonds).reduce((acc, [diamondKey, diamondData]) => {
    // Remove empty/0 fields inside each diamond
    const cleanedDiamond = removeEmptyOrZeroFields(diamondData);

    // If cleaned object is not empty, add to result
    if (Object.keys(cleanedDiamond).length > 0) {
      acc[diamondKey] = cleanedDiamond;
    }

    return acc;
  }, {});
};
