import { httpStatusCodes, tryCatchFn } from "./errorController.js";
import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import Joi from "joi";
import { stullerCategoryIds } from "../helpers/commonConstHelper.js";
import { connect } from "mongoose";

// const ADD_TAG_TO_PRODUCT = `
// mutation addTags($id: ID!, $tags: [String!]!) {
//   tagsAdd(id: $id, tags: $tags) {
//     node {
//       id
//     }
//     userErrors {
//       message
//     }
//   }
// }`;

// const REMOVE_TAG_FROM_PRODUCT = `
// mutation removeTags($id: ID!, $tags: [String!]!) {
//   tagsRemove(id: $id, tags: $tags) {
//     node {
//       id
//     }
//     userErrors {
//       message
//     }
//   }
// }`;

const fetchGroupMetafieldIds = async (syncId) => {
  try {
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

    const response = await graphqlClient(GET_METAFIELDS_QUERY, {
      ownerId: syncId,
    });
    if (!response.success) return { groupId: null };

    if (!response?.data?.product?.metafields?.edges) return { groupId: null };

    const metafields = response.data.product.metafields.edges.map(
      (edge) => edge.node
    );

    const groupId =
      metafields.find((metafield) => metafield.key === "group_name")?.id ||
      null;

    return { groupId };
  } catch (error) {
    console.error(`Error fetching metafield IDs for sync_id: ${syncId}`, error);
    return { groupId: null };
  }
};

const updateShopifyGroupMetafield = async (syncId, groupName) => {
  const { groupId } = await fetchGroupMetafieldIds(syncId);

  // if (groupName !== existingGroupName) {
  //     await graphqlClient(REMOVE_TAG_FROM_PRODUCT,
  //         {
  //             id: syncId,
  //             tags: [existingGroupName]
  //         }
  //     );
  //     await graphqlClient(ADD_TAG_TO_PRODUCT,
  //         {
  //             id: syncId,
  //             tags: groupName
  //         }
  //     );
  // }

  if (!groupId) {
    console.log(`No 'group_name' metafield found for sync_id: ${syncId}`);
    return;
  }

  const UPDATE_META_MUTATION = `
    mutation updateProductMetafields($input: ProductInput!) {
        productUpdate(input: $input) {
            product {
                id
            }
            userErrors {
                message
                field
            }
        }
    }
    `;

  const metafieldToUpdate = {
    id: groupId,
    value: groupName,
  };

  await graphqlClient(UPDATE_META_MUTATION, {
    input: {
      metafields: metafieldToUpdate,
      id: syncId,
    },
  });
};

const fetchStyleMetafieldIds = async (syncId) => {
  try {
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

    const response = await graphqlClient(GET_METAFIELDS_QUERY, {
      ownerId: syncId,
    });

    if (!response.success) return { styleId: null };

    if (!response?.data?.product?.metafields?.edges) return { styleId: null };

    const metafields = response.data.product.metafields.edges.map(
      (edge) => edge.node
    );

    const styleId =
      metafields.find((metafield) => metafield.key === "style")?.id || null;

    return { styleId };
  } catch (error) {
    console.error(
      `Error fetching style metafield ID for sync_id: ${syncId}`,
      error
    );
    return { styleId: null };
  }
};

const updateShopifyStyleMetafield = async (syncId, styleName) => {
  const { styleId } = await fetchStyleMetafieldIds(syncId);

  if (!styleId) {
    console.log(`No 'style' metafield found for sync_id: ${syncId}`);
    return;
  }

  const UPDATE_META_MUTATION = `
    mutation updateProductMetafields($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
        }
        userErrors {
          message
          field
        }
      }
    }`;

  const metafieldToUpdate = {
    id: styleId,
    value: styleName,
  };

  await graphqlClient(UPDATE_META_MUTATION, {
    input: {
      metafields: metafieldToUpdate,
      id: syncId,
    },
  });
};

const assignGroup = tryCatchFn(async (req, res) => {
  const connection = await sqldb.getConnection(); // Define connection variable
  try {
    // Queries
    const queries = {
      rings: `
                SELECT ring_id, supplier_group_id 
                FROM stlr_rings 
                WHERE (group_id IS NULL OR group_id = 0) 
                AND (category_id IS NULL OR category_id = 0);
            `,
      categories: `SELECT * FROM stlr_categories;`,
      styles: `SELECT * FROM stlr_styles;`,
      groups: `
                SELECT g.* 
                FROM stlr_groups g 
                LEFT JOIN stlr_rings r ON g.group_id = r.group_id 
                WHERE r.group_id IS NULL;
            `,
      webCategories: `
                SELECT rhc.ring_id, GROUP_CONCAT(wc.name) AS webcategories 
                FROM stlr_ring_has_categories rhc
                JOIN stlr_webcategories wc ON rhc.web_cat_id = wc.web_cat_id
                WHERE wc.web_cat_id IN (?)
                GROUP BY rhc.ring_id;
            `,
      genders: `SELECT * FROM stlr_genders;`,
    };

    // Execute queries in parallel
    const [ringsData, categories, styles, groups, webCategoriesData, genders] =
      await Promise.all([
        connection.query(queries.rings),
        connection.query(queries.categories),
        connection.query(queries.styles),
        connection.query(queries.groups),
        connection.query(queries.webCategories, [stullerCategoryIds]),
        connection.query(queries.genders),
      ]);

    // Convert comma-separated string to an array
    const webCategoryMap = Object.fromEntries(
      webCategoriesData[0].map(({ ring_id, webcategories }) => [
        ring_id,
        webcategories
          ? webcategories.split(",").map((item) => item.trim())
          : [],
      ])
    );

    // Merge webcategories into rings
    const ringsWithWebCategories = ringsData[0].map((ring) => ({
      ...ring,
      webcategories: webCategoryMap[ring.ring_id] || [],
    }));

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Data fetched successfully.",
      result: {
        rings: ringsWithWebCategories,
        styles: styles[0],
        categories: categories[0],
        groups: groups[0],
        genders: genders[0],
      },
    });
  } catch (error) {
    console.error("Error fetching data:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    // ✅ Release the connection after use
    if (connection) connection.release();
  }
});

const assignedGroup = tryCatchFn(async (req, res) => {
  try {
    // Joi Validation Schema
    const schema = Joi.object({
      page: Joi.number().integer().min(1).default(1),
      limit: Joi.number().integer().min(1).default(20),
      search: Joi.string().trim().allow("").optional(), // Allows empty string but trims whitespace
      sort_field: Joi.string()
        .valid(
          "created_at",
          "supplier_group_id",
          "group_name",
          "category_name",
          "style_name",
          "gender_type"
        )
        .default("created_at"), // Default sorting by created_at
      sort_order: Joi.string().valid("ASC", "DESC").default("DESC"), // Default order is DESC (newest first)
    }).unknown(true); // Allow other query parameters without validation errors

    // Validate query parameters
    const { error, value } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { page, limit, search, sort_field, sort_order } = value;
    const offset = (page - 1) * limit;
    const searchQuery = search ? `%${search}%` : null;

    // Count query with style_id filter
    const countQuery = `
      SELECT COUNT(*) AS total 
      FROM stlr_rings r
      LEFT JOIN stlr_groups g ON r.group_id = g.group_id
      WHERE r.group_id > 0 
        AND r.category_id > 0
        AND r.style_id > 0
        AND r.gender_id > 0
        ${search ? "AND g.group_name LIKE ?" : ""}
    `;
    const countParams = search ? [searchQuery] : [];
    const [[{ total }]] = await sqldb.query(countQuery, countParams);

    if (total === 0) {
      return res.status(200).json({
        status: httpStatusCodes.OK,
        success: true,
        message: "No assigned groups found.",
        result: {
          rings: [],
          page,
          limit,
          total_records: 0,
          total_pages: 0,
        },
      });
    }

    // Main query with style_id filter
    const query = `
      SELECT 
        r.ring_id, 
        r.supplier_group_id, 
        r.group_id, 
        r.category_id,
        r.style_id,
        g.group_name, 
        g.created_at,
        c.name AS category_name,
        s.style_name AS style_name,
        sg.type AS gender_type,
        CASE 
          WHEN EXISTS (
            SELECT 1 FROM stlr_ring_variations rv
            WHERE rv.ring_id = r.ring_id 
              AND rv.sync IS NOT NULL AND rv.sync != '' 
              AND rv.sync_id IS NOT NULL AND rv.sync_id != ''
            LIMIT 1
          ) 
          THEN 0 
          ELSE 1 
        END AS identify
      FROM stlr_rings r
      LEFT JOIN stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN stlr_styles s ON r.style_id = s.style_id
      LEFT JOIN stlr_genders sg ON r.gender_id = sg.gender_id
      WHERE r.group_id > 0 
        AND r.category_id > 0
        AND r.style_id > 0
        AND r.gender_id > 0
        ${search ? "AND g.group_name LIKE ?" : ""}
      ORDER BY 
        ${
          sort_field === "created_at" ? "g.created_at" : sort_field
        } ${sort_order},
        g.created_at DESC
      LIMIT ? OFFSET ?;
    `;

    const queryParams = search ? [searchQuery, limit, offset] : [limit, offset];
    const [rings] = await sqldb.query(query, queryParams);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Assigned groups fetched successfully.",
      result: {
        rings,
        page,
        limit,
        total_records: total,
        total_pages: Math.ceil(total / limit),
      },
    });
  } catch (error) {
    console.error("Error fetching assigned groups:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
});

const allAssignedGroups = tryCatchFn(async (req, res) => {
  try {
    const query = `
      SELECT 
        r.ring_id, 
        r.supplier_group_id, 
        r.group_id, 
        r.category_id,
        r.style_id,
        g.group_name, 
        g.created_at,
        c.name AS category_name,
        s.style_name AS style_name,
        sg.type AS gender_type,
        CASE 
          WHEN EXISTS (
            SELECT 1 FROM stlr_ring_variations rv
            WHERE rv.ring_id = r.ring_id 
              AND rv.sync IS NOT NULL AND rv.sync != '' 
              AND rv.sync_id IS NOT NULL AND rv.sync_id != ''
            LIMIT 1
          ) 
          THEN 0 
          ELSE 1 
        END AS identify
      FROM stlr_rings r
      LEFT JOIN stlr_groups g ON r.group_id = g.group_id
      LEFT JOIN stlr_categories c ON r.category_id = c.category_id
      LEFT JOIN stlr_styles s ON r.style_id = s.style_id
      LEFT JOIN stlr_genders sg ON r.gender_id = sg.gender_id
      WHERE r.group_id > 0 
        AND r.category_id > 0
        AND r.style_id > 0
        AND r.gender_id > 0
      ORDER BY g.group_name ASC;
    `;

    const [rings] = await sqldb.query(query);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Assigned groups fetched successfully.",
      result: rings,
    });
  } catch (error) {
    console.error("Error fetching assigned groups:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
});

const assignGroupEdit = tryCatchFn(async (req, res) => {
  const connection = await sqldb.getConnection();
  try {
    // **1. Joi Validation - Ensure required fields**
    const schema = Joi.object({
      supplier_group_id: Joi.string().required(),
      category_id: Joi.number().integer().min(1).required(),
      group_id: Joi.number().integer().min(1).required(),
      style_id: Joi.number().integer().min(1).required(),
      gender_id: Joi.number().integer().min(1).required(),
    });

    const { error } = schema.validate(req.body);
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { supplier_group_id, category_id, group_id, style_id, gender_id } =
      req.body;

    // ** 2. Check if group_id exists in stlr_groups table**
    const [[groupExists]] = await connection.query(
      `SELECT group_id, group_name FROM stlr_groups WHERE group_id = ?`,
      [group_id]
    );

    if (!groupExists) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Invalid group_id. No matching group found.",
      });
    }

    // ** 3. Check if style_id exists in stlr_styles table **
    const [[styleExists]] = await connection.query(
      `SELECT style_id, style_name FROM stlr_styles WHERE style_id = ?`,
      [style_id]
    );

    if (!styleExists) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Invalid style_id. No matching style found.",
      });
    }

    // ** 4. Check if group_id is already assigned to another ring**
    const [[existingGroupRing]] = await connection.query(
      `SELECT ring_id FROM stlr_rings 
                WHERE group_id = ? AND supplier_group_id != ? 
                LIMIT 1;`,
      [group_id, supplier_group_id]
    );

    if (existingGroupRing) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "This group_id is already assigned to another ring.",
      });
    }

    // ** 5. Check if gender_id exists in stlr_gender table**
    const [[genderExists]] = await connection.query(
      `SELECT gender_id, type FROM stlr_genders WHERE gender_id = ?`,
      [gender_id]
    );

    if (!genderExists) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Invalid gender_id. No matching gender found.",
      });
    }

    // ** 6. Check if ring_id or stuller_group_id exists in the stlr_rings table or not. If exists then fetch ring details and check existing category/style/group**
    const ringQuery = `
            SELECT ring_id, category_id, style_id, group_id, gender_id
            FROM stlr_rings 
            WHERE supplier_group_id = ? 
            LIMIT 1;
        `;
    const [ringCheck] = await connection.query(ringQuery, [supplier_group_id]);

    if (ringCheck.length === 0) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "No matching ring found for the given supplier_group_id.",
      });
    }

    const {
      ring_id,
      category_id: existingCategoryId,
      style_id: existingStyleId,
      group_id: existingGroupId,
      gender_id: existingGenderId,
    } = ringCheck[0];

    // ** 7. Check if new category/group/style/gender are the same as existing**
    if (
      existingCategoryId == category_id &&
      existingStyleId == style_id &&
      existingGroupId == group_id &&
      existingGenderId == gender_id
    ) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "New and existing category/style/group are the same.",
      });
    }

    // ** 8. Check if all sync and sync_id values in stlr_ring_variations are null or empty for this ring_id**
    const syncCheckQuery = `
            SELECT COUNT(*) AS total, 
                SUM(CASE WHEN sync IS NOT NULL AND sync != '' THEN 1 ELSE 0 END) AS sync_count, 
                SUM(CASE WHEN sync_id IS NOT NULL AND sync_id != '' THEN 1 ELSE 0 END) AS sync_id_count
            FROM stlr_ring_variations 
            WHERE ring_id = ?;
            `;

    const [[{ sync_count, sync_id_count }]] = await connection.query(
      syncCheckQuery,
      [ring_id]
    );

    console.log("sync_count", typeof sync_count);
    console.log("sync_id_count", sync_id_count);

    if (sync_count == 0 && sync_id_count == 0) {
      await connection.query(
        `UPDATE stlr_rings SET category_id = ?, style_id = ?, group_id = ?, gender_id = ? WHERE ring_id = ?;`,
        [category_id, style_id, group_id, gender_id, ring_id]
      );

      return res.status(200).json({
        status: httpStatusCodes.OK,
        success: true,
        message: "Group, category, tyle and gender assigned successfully.",
      });
    }

    // ** 9. Fetch sync_ids (Allow updates even if they exist)**
    const [syncRows] = await connection.execute(
      `SELECT sync_id FROM stlr_ring_variations 
             WHERE ring_id = ? AND sync_id IS NOT NULL AND sync_id != ''`,
      [ring_id]
    );

    if (syncRows.length === 0) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "No valid sync IDs found for the given Supplier Group ID.",
      });
    }

    const existingSyncIds = syncRows.map((row) => row.sync_id);

    // ** 10. Fetch new category Shopify ID**
    const [[newCollection]] = await connection.execute(
      `SELECT shopify_id,name FROM stlr_categories WHERE category_id = ?`,
      [category_id]
    );

    if (!newCollection || !newCollection.shopify_id) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "New category not found in Shopify.",
      });
    }

    // ** 11. Fetch existing collection ID for the supplier_group_id (if it exists)**
    const [[existingCollection]] = await connection.execute(
      `SELECT c.shopify_id 
             FROM stlr_categories c
             JOIN stlr_rings r ON c.category_id = r.category_id
             WHERE r.supplier_group_id = ?`,
      [supplier_group_id]
    );

    const existingCollectionId = existingCollection
      ? existingCollection.shopify_id
      : null;

    // ** 12. Check if category & style & group are unchanged**
    if (
      existingCollectionId == newCollection.shopify_id &&
      existingStyleId == style_id &&
      existingGroupId == group_id &&
      existingGenderId == gender_id
    ) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "New and existing categories/style/group/gender are the same.",
      });
    }

    // ** 12. Update Shopify Products. Old unwanted code**
    // if (existingSyncIds.length > 0) {
    //   // **Verify new collection exists in Shopify**
    //   const COLLECTION_CHECK_QUERY = `
    //         query CheckCollection($id: ID!) {
    //             collection(id: $id) { id title }
    //         }`;
    //   const checkCollection = await graphqlClient(COLLECTION_CHECK_QUERY, {
    //     id: newCollection.shopify_id,
    //   });

    //   if (!checkCollection.success || !checkCollection.data.collection) {
    //     return res.status(404).json({
    //       status: httpStatusCodes.NOT_FOUND,
    //       success: false,
    //       message: "New Shopify collection does not exist.",
    //     });
    //   }

    //   // **Verify products exist in Shopify**
    //   const GET_PRODUCT_QUERY = `
    //             query GetProduct($id: ID!) {
    //                 product(id: $id) {
    //                     id
    //                     title
    //                 }
    //             }
    //         `;

    //   let nonExistingProducts = [];

    //   for (const sync_id of existingSyncIds) {
    //     try {
    //       const response = await graphqlClient(GET_PRODUCT_QUERY, {
    //         id: sync_id,
    //       });

    //       if (!response.success || !response.data.product) {
    //         nonExistingProducts.push(sync_id);
    //       }
    //     } catch (error) {
    //       console.error(
    //         `Error checking product existence for sync_id: ${sync_id}`,
    //         error
    //       );
    //       nonExistingProducts.push(sync_id);
    //     }
    //   }

    //   if (nonExistingProducts.length > 0) {
    //     return res.status(400).json({
    //       status: httpStatusCodes.BAD_REQUEST,
    //       success: false,
    //       message: `The following products do not exist in Shopify: ${nonExistingProducts.join(
    //         ", "
    //       )}`,
    //       result: nonExistingProducts,
    //     });
    //   }

    //   // **Remove products from existing collection (if exists)**
    //   if (existingCollectionId) {
    //     const COLLECTION_REMOVE_PRODUCT_MUTATION = `
    //             mutation collectionRemoveProducts($id: ID!, $productIds: [ID!]!) {
    //                 collectionRemoveProducts(id: $id, productIds: $productIds) {
    //                     job { done id }
    //                     userErrors { field message }
    //                 }
    //             }
    //             `;
    //     const removeProductResponse = await graphqlClient(
    //       COLLECTION_REMOVE_PRODUCT_MUTATION,
    //       {
    //         id: existingCollectionId,
    //         productIds: existingSyncIds,
    //       }
    //     );

    //     if (
    //       !removeProductResponse.success ||
    //       removeProductResponse.data.collectionRemoveProducts.userErrors
    //         .length > 0
    //     ) {
    //       return res.status(400).json({
    //         status: httpStatusCodes.BAD_REQUEST,
    //         success: false,
    //         message: "Failed to remove products from the existing collection.",
    //       });
    //     }
    //   }

    //   // ** Add products to the new collection**
    //   const COLLECTION_ADD_PRODUCT_MUTATION = `
    //             mutation collectionAddProducts($id: ID!, $productIds: [ID!]!) {
    //                 collectionAddProducts(id: $id, productIds: $productIds) {
    //                     collection { id title }
    //                     userErrors { field message }
    //                 }
    //             }
    //         `;
    //   const addProductResponse = await graphqlClient(
    //     COLLECTION_ADD_PRODUCT_MUTATION,
    //     {
    //       id: newCollection.shopify_id,
    //       productIds: existingSyncIds,
    //     }
    //   );

    //   if (
    //     !addProductResponse.success ||
    //     addProductResponse.data.collectionAddProducts.userErrors.length > 0
    //   ) {
    //     return res.status(400).json({
    //       status: httpStatusCodes.BAD_REQUEST,
    //       success: false,
    //       message: "Failed to add products to the new collection.",
    //     });
    //   }

    //   // **Update Shopify Metafields**
    //   const { group_name } = groupExists;

    //   let failedSyncIds = [];

    //   for (const sync_id of existingSyncIds) {
    //     try {
    //       await updateShopifyGroupMetafield(sync_id, group_name);
    //     } catch (error) {
    //       console.error(
    //         `Failed to update metafield for sync_id: ${sync_id}`,
    //         error
    //       );
    //       failedSyncIds.push(sync_id);
    //     }
    //   }

    //   if (failedSyncIds.length > 0) {
    //     return res.status(500).json({
    //       status: httpStatusCodes.INTERNAL_SERVER,
    //       success: false,
    //       message: `Failed to update metafields for sync_ids: ${failedSyncIds.join(
    //         ", "
    //       )}`,
    //       failedSyncIds,
    //     });
    //   }

    //   // ** Update the metafield "style" for the products **
    //   const { style_name } = styleExists;

    //   let failedStyleSyncIds = [];

    //   for (const sync_id of existingSyncIds) {
    //     try {
    //       await updateShopifyStyleMetafield(sync_id, style_name);
    //     } catch (error) {
    //       console.error(
    //         `Failed to update style metafield for sync_id: ${sync_id}`,
    //         error
    //       );
    //       failedStyleSyncIds.push(sync_id);
    //     }
    //   }

    //   if (failedStyleSyncIds.length > 0) {
    //     return res.status(500).json({
    //       status: httpStatusCodes.INTERNAL_SERVER,
    //       success: false,
    //       message: `Failed to update style metafields for sync_ids: ${failedStyleSyncIds.join(
    //         ", "
    //       )}`,
    //       failedSyncIds: failedStyleSyncIds,
    //     });
    //   }
    // }

    // ** 12. Throw validation error if the variant is already synced**
    if (existingSyncIds.length > 0) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message:
          "Sync with Shopify exists for this ring. Cannot update group/category/style/gender.",
      });
    }

    // ** 13. Update category_id, group_id for the ring**
    await connection.query(
      `UPDATE stlr_rings SET category_id = ?, style_id = ?, group_id = ?, gender_id = ? WHERE supplier_group_id = ?;`,
      [category_id, style_id, group_id, gender_id, supplier_group_id]
    );

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Category/Group/Style/Gender updated successfully.",
    });
  } catch (error) {
    console.error("Error updating assigned group:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
});

const assignedGroupById = tryCatchFn(async (req, res) => {
  let connection; // Define connection variable

  try {
    const { id } = req.params; // Extract supplier_group_id from URL

    // ✅ Joi Validation: Ensure id is a valid string (non-empty)
    const schema = Joi.object({
      id: Joi.string().trim().required(),
    });

    const { error } = schema.validate({ id });
    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Invalid supplier_group_id. It must be a non-empty string. ${error.details.map(
          (err) => err.message
        )}`,
      });
    }

    // ✅ Get a database connection
    connection = await sqldb.getConnection();

    // ✅ Query to get ONE ring assigned to the given supplier_group_id
    const ringQuery = `
            SELECT 
                r.ring_id, 
                r.group_id, 
                r.supplier_group_id, 
                g.group_name, 
                r.category_id, 
                c.name AS category_name,
                r.style_id,
                s.style_name AS style_name,
                sg.gender_id,
                sg.type AS gender_type
            FROM stlr_rings r
            LEFT JOIN stlr_groups g ON r.group_id = g.group_id
            LEFT JOIN stlr_categories c ON r.category_id = c.category_id
            LEFT JOIN stlr_styles s ON r.style_id = s.style_id
            LEFT JOIN stlr_genders sg ON r.gender_id = sg.gender_id
            WHERE r.supplier_group_id = ?
            LIMIT 1;
        `;

    // ✅ Query to get all categories
    const categoriesQuery = `SELECT * FROM stlr_categories;`;

    // ✅ Query to get all styles
    const stylesQuery = `SELECT * FROM stlr_styles;`;

    // ✅ Query to get unassigned groups, including assigned ones for the supplier_group_id
    const groupsQuery = `
            SELECT g.* 
            FROM stlr_groups g
            LEFT JOIN stlr_rings r ON g.group_id = r.group_id 
            WHERE r.group_id IS NULL OR r.supplier_group_id = ?;
        `;

    // ✅ Query to get all genders
    const gendersQuery = `SELECT * FROM stlr_genders;`;

    // ✅ Execute queries using the connection
    const [[rings], [categories], [styles], [groups], [genders]] =
      await Promise.all([
        connection.query(ringQuery, [id]),
        connection.query(categoriesQuery),
        connection.query(stylesQuery),
        connection.query(groupsQuery, [id]),
        connection.query(gendersQuery),
      ]);

    // ✅ If no ring is found, return a 404 error
    if (!rings.length) {
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "No ring found for this supplier_group_id.",
        result: [],
      });
    }

    // ✅ Get the first ring from the result
    const selectedRing = rings[0];

    // ✅ Fetch web categories for the selected ring
    const webCategoriesQuery = `
            SELECT wc.name
            FROM stlr_ring_has_categories rhc
            JOIN stlr_webcategories wc ON rhc.web_cat_id = wc.web_cat_id
            WHERE rhc.ring_id = ? AND wc.web_cat_id IN (?);
        `;

    const [webCategoriesResult] = await connection.query(webCategoriesQuery, [
      selectedRing.ring_id,
      stullerCategoryIds,
    ]);

    const webCategories = webCategoriesResult.map((category) => category.name);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Assigned ring, categories and groups fetched successfully.",
      result: {
        rings: [
          {
            supplier_group_id: selectedRing.supplier_group_id,
            rings: selectedRing.ring_id,
            webcategories: webCategories,
          },
        ],
        categories,
        styles,
        groups,
        genders,
        ringData: rings,
      },
    });
  } catch (error) {
    console.error("Error fetching assigned group data:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    // ✅ Release the connection after use
    if (connection) connection.release();
  }
});

export {
  assignGroup,
  assignedGroup,
  allAssignedGroups,
  assignGroupEdit,
  assignedGroupById,
};
