import { httpStatusCodes } from "./errorController.js";
import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import Joi from "joi";
import axios from "axios";
import FormData from "form-data";

const getStyle = async (req, res) => {
  try {
    // Define Joi schema for request body validation
    const schema = Joi.object({
      search: Joi.string().allow("").optional(),
      sort: Joi.string().valid("ASC", "DESC").default("ASC"),
      page: Joi.number().integer().min(1).default(1),
      limit: Joi.number().integer().min(1).max(100).default(20),
    });

    // Validate request body
    const { error, value } = schema.validate(req.body);

    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { search, sort, page, limit } = value;
    const offset = (page - 1) * limit;

    // Prepare search condition
    let whereClause = "";
    const params = [];
    if (search) {
      whereClause = " WHERE s.style_name LIKE ? ";
      params.push(`%${search}%`);
    }

    // Fetch paginated styles
    const query = `
      SELECT 
        s.*
      FROM stlr_styles s
      ${whereClause}
      ORDER BY s.style_name ${sort}
      LIMIT ? OFFSET ?;
    `;

    params.push(limit, offset);
    const [styles] = await sqldb.query(query, params);

    // Fetch total count for pagination
    const countQuery = `SELECT COUNT(*) AS total FROM stlr_styles s ${whereClause}`;
    const countParams = search ? [`%${search}%`] : [];
    const [[countResult]] = await sqldb.query(countQuery, countParams);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Styles fetched successfully.",
      result: styles,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(countResult.total / limit),
        totalRecords: countResult.total,
      },
    });
  } catch (error) {
    console.error("Error fetching styles:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const getStylesByIds = async (req, res) => {
  const schema = Joi.object({
    style_ids: Joi.array()
      .items(Joi.number().integer().positive().required())
      .min(1)
      .required()
      .messages({
        "array.base": "style_ids must be an array.",
        "array.min": "At least one style_id must be provided.",
        "any.required": "style_ids array is required.",
      }),
  });

  const { error } = schema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: 400,
      success: false,
      message: error.details[0].message,
    });
  }

  const { style_ids } = req.body;

  const connection = await sqldb.getConnection();
  try {
    const [styles] = await connection.execute(
      `SELECT * FROM wedding_band.stlr_styles 
       WHERE style_id IN (${style_ids.map(() => "?").join(", ")})`,
      style_ids
    );

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Styles fetched successfully.",
      result: styles,
    });
  } catch (error) {
    console.error("Error fetching styles:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const createStyle = async (req, res) => {
  // Joi schema for style_name validation
  const styleSchema = Joi.object({
    style_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/) // Prevents purely numeric names
      .required()
      .messages({
        "string.empty": "Style name is required.",
        "string.min": "Style name must be at least 2 characters long.",
        "string.max": "Style name cannot exceed 100 characters.",
        "string.pattern.base": "Style name cannot be only numbers.",
      }),
  });

  const { error } = styleSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: error.details[0].message,
    });
  }

  const { style_name } = req.body;
  const files = req.files || [];

  if (!style_name) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: "Style name is required",
    });
  }

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if style name already exists
    const [existingStyle] = await connection.execute(
      `SELECT style_id FROM wedding_band.stlr_styles WHERE style_name = ?`,
      [style_name]
    );

    if (existingStyle.length > 0) {
      return res.status(409).json({
        status: 409,
        success: false,
        message: "Style name already exists",
      });
    }

    /**
     * Update the predefined values present in the metafield definition called "Style".
     */
    const STYLE_DEFINITION_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(STYLE_DEFINITION_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );

    const styleMetafield = definitions.find((def) => def.name === "Style");
    if (!styleMetafield) {
      throw new Error("Style metafield definition not found");
    }

    const styleMetafieldId = styleMetafield.id;

    const STYLE_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${styleMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(STYLE_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
    const validations = definition?.validations || [];

    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        console.error(
          "Invalid choices JSON format, initializing to empty list."
        );
        currentChoices = [];
      }
    }

    if (!currentChoices.includes(style_name)) {
      currentChoices.push(style_name);
    }

    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: styleMetafield.namespace,
        key: styleMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(currentChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    if (updateResponse?.errors) {
      console.error("GraphQL errors:", updateResponse.errors);
      throw new Error("GraphQL mutation failed");
    }

    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;
    if (userErrors && userErrors.length > 0) {
      console.error("Shopify update errors:", userErrors);
      throw new Error("Failed to update Shopify style list");
    }

    /**
     * Update predefined choices for the field "Style" in the metaobject definition "Sub Collection Urls".
     */
    const GET_METAOBJECT_DEFINITIONS = `
      query {
        metaobjectDefinitions(first: 100) {
          edges {
            node {
              id
              name
              fieldDefinitions {
                key
                name
                type {
                  name
                }
                required
                validations {
                  name
                  value
                }
              }
            }
          }
        }
      }
    `;

    // Fetch all metaobject definitions
    const metaobjectDefsResponse = await graphqlClient(
      GET_METAOBJECT_DEFINITIONS
    );

    const metaobjectDefs =
      metaobjectDefsResponse?.data?.metaobjectDefinitions?.edges?.map(
        (edge) => edge.node
      );

    if (!metaobjectDefs || metaobjectDefs.length === 0) {
      throw new Error("No metaobject definitions found");
    }

    // Find the "Sub Collection Urls" metaobject definition by name
    const subCollectionMetaobjectDef = metaobjectDefs.find(
      (def) => def.name === "Sub Collection Urls"
    );

    if (!subCollectionMetaobjectDef) {
      throw new Error('"Sub Collection Urls" metaobject definition not found');
    }

    const styleField = subCollectionMetaobjectDef.fieldDefinitions.find(
      (field) => field.key === "style"
    );

    if (!styleField) {
      throw new Error(
        'Style field not found in "Sub Collection Urls" metaobject'
      );
    }

    let currentChoicesForStylefield = [];

    const choicesValidationForStyle = styleField.validations.find(
      (v) => v.name === "choices"
    );

    if (choicesValidationForStyle && choicesValidationForStyle.value) {
      try {
        currentChoicesForStylefield = JSON.parse(
          choicesValidationForStyle.value
        );
      } catch (e) {
        console.warn(
          "Invalid choices JSON in style field, resetting to empty array."
        );
        currentChoicesForStylefield = [];
      }
    }

    // Add new style_name if not already included
    if (!currentChoicesForStylefield.includes(style_name)) {
      currentChoicesForStylefield.push(style_name);
    }

    // Preserve other validations (excluding "choices")
    const otherValidations = styleField.validations.filter(
      (v) => v.name !== "choices"
    );

    // Add updated choices validation
    const updatedValidations = [
      ...otherValidations,
      {
        name: "choices",
        value: JSON.stringify(currentChoicesForStylefield),
      },
    ];

    const UPDATE_METAOBJECT_DEFINITION = `
      mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
        metaobjectDefinitionUpdate(id: $id, definition: $definition) {
          metaobjectDefinition {
            id
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const definitionInput = {
      fieldDefinitions: {
        update: {
          key: "style",
          validations: updatedValidations,
        },
      },
    };

    const updateResponseForMetaObject = await graphqlClient(
      UPDATE_METAOBJECT_DEFINITION,
      {
        id: subCollectionMetaobjectDef.id,
        definition: definitionInput,
      }
    );

    const errors =
      updateResponseForMetaObject?.data?.metaobjectDefinitionUpdate?.userErrors;

    if (errors && errors.length > 0) {
      throw new Error(`Failed to update field: ${JSON.stringify(errors)}`);
    } else {
      console.log("Style field updated successfully.");
    }

    /**
     * If there is no file uploaded, then create a metaobject with the style name only.
     */
    if (files.length === 0) {
      const CREATE_METAOBJECT_MUTATION = `
        mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              type
              fields {
                key
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const metaobjectInput = {
        type: "filter_images",
        fields: [
          { key: "name", value: style_name },
          { key: "type", value: "Style" },
          // No image field since no file was uploaded
        ],
      };

      const metaobjectResponse = await graphqlClient(
        CREATE_METAOBJECT_MUTATION,
        { metaobject: metaobjectInput }
      );

      if (
        metaobjectResponse?.data?.metaobjectCreate?.userErrors &&
        metaobjectResponse.data.metaobjectCreate.userErrors.length > 0
      ) {
        throw new Error(
          `Metaobject creation failed: ${JSON.stringify(
            metaobjectResponse.data.metaobjectCreate.userErrors
          )}`
        );
      }

      const createdMetaobject =
        metaobjectResponse?.data?.metaobjectCreate?.metaobject;
    } else if (files.length > 0) {
      /**
       * If the file is uploaded, then upload the file to the Shopify and create a metaobject with the style name and image url.
       */
      files.forEach((file) => {
        const extension = file.originalname.split(".").pop();
        file.renamedTo = `${style_name}.${extension}`;
      });

      // This api takes only one image only. So we will take the first image from the files array.
      const file = files[0];

      /**
       * Upload the file to Shopify using Staged Uploads API and get the URL.
       */
      const stagedUploadsMutation = `
          mutation generateStagedUploadTarget($input: [StagedUploadInput!]!) {
            stagedUploadsCreate(input: $input) {
              stagedTargets {
                url
                resourceUrl
                parameters {
                  name
                  value
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

      const uploadInput = [
        {
          filename: file.renamedTo,
          mimeType: file.mimetype,
          resource: "FILE",
          httpMethod: "POST",
        },
      ];

      // Get the upload URL and parameters from Shopify
      const stagedUploadResponse = await graphqlClient(stagedUploadsMutation, {
        input: uploadInput,
      });

      const stagedTarget =
        stagedUploadResponse?.data?.stagedUploadsCreate?.stagedTargets?.[0];

      if (!stagedTarget) {
        throw new Error("Failed to get staged upload target from Shopify");
      }

      // Prepare form-data to upload the file buffer
      const form = new FormData();
      for (const param of stagedTarget.parameters) {
        form.append(param.name, param.value);
      }
      form.append("file", file.buffer, file.renamedTo);

      // Upload the file to Shopify's storage
      await axios.post(stagedTarget.url, form, {
        headers: form.getHeaders(),
        maxBodyLength: Infinity,
      });

      // Save the uploaded image URL for later use
      file.uploadedUrl = stagedTarget.resourceUrl;

      /**
       * Create the file object using the uploaded file URL. Then use it for the metaobject creation.
       */
      const FILE_CREATE_MUTATION = `
        mutation fileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const filesCreateInput = [
        {
          originalSource: file.uploadedUrl, // This is the resourceUrl from staged upload
          contentType: "IMAGE",
          alt: file.originalname,
        },
      ];

      // Create the file object in Shopify and get the file ID and permanent URL
      const filesCreateResponse = await graphqlClient(FILE_CREATE_MUTATION, {
        files: filesCreateInput,
      });
      const createdFile = filesCreateResponse?.data?.fileCreate?.files?.[0];
      const fileId = createdFile?.id;

      // Wait 3 second for Shopify to index the file
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Use nodes(ids: $ids) to fetch the permanent URL
      const FILE_QUERY = `
        query getFile($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on MediaImage {
              id
              image {
                url
              }
            }
            ... on GenericFile {
              id
              url
            }
          }
        }
      `;

      const fileQueryResponse = await graphqlClient(FILE_QUERY, {
        ids: [fileId],
      });

      const fileNode = fileQueryResponse?.data?.nodes?.[0];

      let permanentUrl = null;

      if (fileNode?.image?.url) {
        permanentUrl = fileNode.image.url;
      } else if (fileNode?.url) {
        permanentUrl = fileNode.url;
      }

      if (!permanentUrl) {
        throw new Error(
          "Failed to fetch permanent URL for uploaded file from Shopify"
        );
      }

      // Use the permanent URL for DB and further logic
      file.uploadedUrl = permanentUrl;

      /**
       * Create the metaobject with the style name and image url.
       */
      const CREATE_METAOBJECT_MUTATION = `
        mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              type
              fields {
                key
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const metaobjectInput = {
        type: "filter_images",
        fields: [
          { key: "name", value: style_name },
          { key: "type", value: "Style" },
          { key: "image", value: fileId },
        ],
      };

      const metaobjectResponse = await graphqlClient(
        CREATE_METAOBJECT_MUTATION,
        { metaobject: metaobjectInput }
      );

      if (
        metaobjectResponse?.data?.metaobjectCreate?.userErrors &&
        metaobjectResponse.data.metaobjectCreate.userErrors.length > 0
      ) {
        throw new Error(
          `Metaobject creation failed: ${JSON.stringify(
            metaobjectResponse.data.metaobjectCreate.userErrors
          )}`
        );
      }

      const createdMetaobject =
        metaobjectResponse?.data?.metaobjectCreate?.metaobject;
    }

    /**
     * Insert into database
     */
    const [result] = await connection.execute(
      `INSERT INTO wedding_band.stlr_styles (style_name, image) VALUES (?, ?)`,
      [style_name, files.length > 0 ? files[0].uploadedUrl : null]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Style created successfully",
      result: result.insertId,
    });
  } catch (error) {
    console.error("Error is", error);
    return res.status(500).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const updateSingleStyle = async (req, res) => {
  const updateStyleSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Style ID must be a number.",
      "number.integer": "Style ID must be an integer.",
      "number.positive": "Style ID must be a positive number.",
      "any.required": "Style ID is required.",
    }),
  });

  // Schema for validating the style name
  const styleNameSchema = Joi.object({
    style_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/)
      .required()
      .messages({
        "string.empty": "Style name is required.",
        "string.min": "Style name must be at least 2 characters long.",
        "string.max": "Style name cannot exceed 100 characters.",
        "string.pattern.base": "Style name cannot be only numbers.",
      }),
    existing_url: Joi.string().allow("").required().messages({
      "string.base": "Existing URL must be a string.",
      "any.required": "Existing URL is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = updateStyleSchema.validate({ id: numericId });
  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const { error: nameError } = styleNameSchema.validate(req.body);
  if (nameError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: nameError.details[0].message,
    });
  }

  const { style_name } = req.body;
  const files = req.files || [];
  let existing_url = req.body.existing_url;

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if style exists
    const [styles] = await connection.query(
      "SELECT * FROM stlr_styles WHERE style_id = ? LIMIT 1",
      [numericId]
    );

    if (styles.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Style not found. Please provide a valid style ID.",
      });
    }

    const oldStyleName = styles[0].style_name;
    const oldImageUrl = styles[0].image;

    // Check for duplicate style name
    const [existingStyle] = await connection.query(
      "SELECT style_id FROM stlr_styles WHERE style_name = ? AND style_id != ? LIMIT 1",
      [style_name, numericId]
    );

    if (existingStyle.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Style name already exists. Please choose a different name.",
      });
    }

    // Validate that existing_url is either empty string or matches oldImageUrl
    if (existing_url !== "" && existing_url !== oldImageUrl) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "existing_url must be empty or match the current image URL.",
      });
    }

    /**
     * Keep the filtered metaobjects of type "filter_images". This list is kept globally for name update and image update
     */
    const METAOBJECTS_BY_TYPE_QUERY = `
        query MetaobjectsByType($type: String!) {
          metaobjects(first: 100, type: $type) {
            edges {
              node {
                id
                type
                fields {
                  key
                  value
                }
              }
            }
          }
        }
    `;

    const metaobjectsResponse = await graphqlClient(METAOBJECTS_BY_TYPE_QUERY, {
      type: "filter_images",
    });

    const metaobjects =
      metaobjectsResponse?.data?.metaobjects?.edges?.map((edge) => edge.node) ||
      [];

    // Filter metaobjects whose "name" field matches oldStyleName and "type" field is "Style"
    const filteredMetaobjects = metaobjects.filter((metaobject) => {
      if (!Array.isArray(metaobject.fields)) return false;
      const nameField = metaobject.fields.find((f) => f.key === "name");
      const typeField = metaobject.fields.find((f) => f.key === "type");
      return (
        nameField &&
        typeof nameField.value === "string" &&
        nameField.value.trim() === oldStyleName.trim() &&
        typeField &&
        typeof typeField.value === "string" &&
        typeField.value.trim() === "Style"
      );
    });

    // If no metaobjects found, it means manually someone has deleted the metaobject. We will throw the error.
    if (filteredMetaobjects.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: `You tried to update ${oldStyleName}.No metaobject found in the shopify with ${oldStyleName}. Can not be updated. Data mismatch. Please contact support.`,
      });
    }

    /**
     * Perform the shopify operations only if the new style name and old style name are different
     */
    if (oldStyleName.trim() !== style_name.trim()) {
      /**
       * Update the style name in the predefined style names list.
       */
      // Get the Style Name metafield id
      const METAFIELD_DEFINITIONS_QUERY = `
        query {
          metafieldDefinitions(first: 100, ownerType: PRODUCT) {
            edges {
              node {
                id
                name
                namespace
                key
                validations {
                  name
                  value
                }
              }
            }
          }
        }
      `;

      const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
      const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
        (edge) => edge.node
      );
      const styleMetafield = definitions.find((def) => def.name === "Style");

      if (!styleMetafield) {
        throw new Error("Style metafield definition not found");
      }

      const styleMetafieldId = styleMetafield.id;

      // Get the existing style names from the metafield "validations"
      const METAFIELD_PREDEFINED_VALUES_QUERY = `
        query {
          metafieldDefinition(id: "${styleMetafieldId}") {
            id
            name
            namespace
            key
            validations {
              name
              value
            }
          }
        }
      `;

      const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
      const definition =
        response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
      const validations = definition?.validations || [];

      const choicesValidation = validations.find((v) => v.name === "choices");
      let currentChoices = [];

      if (choicesValidation && choicesValidation.value) {
        try {
          currentChoices = JSON.parse(choicesValidation.value);
        } catch (e) {
          currentChoices = [];
        }
      }

      // Replace the old name with the new style name in the choices array
      let updatedChoices = currentChoices.map((choice) =>
        choice === oldStyleName ? style_name : choice
      );

      updatedChoices = [...new Set(updatedChoices)];

      // Update the metafield definition with the new list of style names
      const UPDATE_ENUM_VALUES_MUTATION = `
        mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
          metafieldDefinitionUpdate(definition: $definition) {
            updatedDefinition {
              id
              validations {
                name
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const variables = {
        definition: {
          namespace: styleMetafield.namespace,
          key: styleMetafield.key,
          ownerType: "PRODUCT",
          validations: [
            {
              name: "choices",
              value: JSON.stringify(updatedChoices),
            },
          ],
        },
      };

      const updateResponse = await graphqlClient(
        UPDATE_ENUM_VALUES_MUTATION,
        variables
      );

      if (updateResponse?.errors) {
        throw new Error("GraphQL mutation failed");
      }

      const userErrors =
        updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

      if (userErrors && userErrors.length > 0) {
        throw new Error("Failed to update metafield list");
      }

      /**
       *  All the products which were present in the shopify with old style name should update with the new style name
       */
      // Get all the products which have metafield "Style" and "Style is equal to oldStyleName"
      const PRODUCT_WITH_STYLE_NAME_QUERY = `
        query {
          products(first: 100) {
            edges {
              node {
                id
                title
                metafields(first: 10, namespace: "custom") {
                  edges {
                    node {
                      key
                      value
                    }
                  }
                }
              }
            }
          }
        }
      `;

      const productCheckResponse = await graphqlClient(
        PRODUCT_WITH_STYLE_NAME_QUERY
      );

      const productEdges = productCheckResponse?.data?.products?.edges || [];

      const productsToUpdate = productEdges.filter((edge) => {
        const metafields =
          edge.node.metafields?.edges?.map((m) => m.node) || [];
        return metafields.some(
          (mf) => mf.key === "style" && mf.value.trim() === oldStyleName.trim()
        );
      });

      // Update the products and maintain two arrays one for successful updates and one for failed updates.
      // If we get failed updates after the update process, then revert all updated products and throw error.
      if (productsToUpdate.length > 0) {
        const successfulUpdates = [];
        const failedUpdates = [];

        for (const product of productsToUpdate) {
          const updateMutation = `
            mutation {
              metafieldsSet(metafields: [{
                namespace: "custom",
                key: "style",
                ownerId: "${product.node.id}",
                value: "${style_name}",
                type: "single_line_text_field"
              }]) {
                metafields {
                  id
                  key
                  value
                }
                userErrors {
                  field
                  message
                }
              }
            }
          `;

          try {
            const updateResponse = await graphqlClient(updateMutation);
            const errors =
              updateResponse?.data?.metafieldsSet?.userErrors || [];

            if (errors.length > 0) {
              console.error(
                `Failed to update product ${product.node.title}:`,
                errors
              );
              failedUpdates.push(product);
            } else {
              successfulUpdates.push(product);
            }
          } catch (err) {
            console.error(
              `Error updating product ${product.node.title}:`,
              err.message
            );
            failedUpdates.push(product);
          }
        }

        // If there are failed updates, revert successful updates and throw error
        if (failedUpdates.length > 0) {
          for (const successfulProduct of successfulUpdates) {
            const revertMutation = `
              mutation {
                metafieldsSet(metafields: [{
                  namespace: "custom",
                  key: "style",
                  ownerId: "${successfulProduct.node.id}",
                  value: "${oldStyleName}",
                  type: "single_line_text_field"
                }]) {
                  metafields {
                    id
                    key
                    value
                  }
                  userErrors {
                    field
                    message
                  }
                }
              }
            `;

            try {
              await graphqlClient(revertMutation);
              console.log(
                `Reverted product ${successfulProduct.node.title} to old style name.`
              );
            } catch (err) {
              console.error(
                `Error reverting product ${successfulProduct.node.title}:`,
                err.message
              );
            }
          }

          const failedProductTitles = failedUpdates
            .map((p) => p.node.title)
            .join(", ");
          console.error(
            `Failed to update the following products: ${failedProductTitles}`
          );

          throw new Error(
            `Metafield predefined list got updated but Failed to update some products. Rolled back changes for the products. Please check the logs.`
          );
        } else {
          console.log("All products updated successfully.");
        }
      }

      /**
       * Update predefined choices for the field "Style" in the metaobject definition "Sub Collection Urls".
       */
      const GET_METAOBJECT_DEFINITIONS = `
        query {
          metaobjectDefinitions(first: 100) {
            edges {
              node {
                id
                name
                fieldDefinitions {
                  key
                  name
                  type {
                    name
                  }
                  required
                  validations {
                    name
                    value
                  }
                }
              }
            }
          }
        }
      `;

      // Fetch all metaobject definitions
      const metaobjectDefsResponse = await graphqlClient(
        GET_METAOBJECT_DEFINITIONS
      );

      const metaobjectDefs =
        metaobjectDefsResponse?.data?.metaobjectDefinitions?.edges?.map(
          (edge) => edge.node
        );

      if (!metaobjectDefs || metaobjectDefs.length === 0) {
        throw new Error("No metaobject definitions found");
      }

      // Find the "Sub Collection Urls" metaobject definition by name
      const subCollectionMetaobjectDef = metaobjectDefs.find(
        (def) => def.name === "Sub Collection Urls"
      );

      if (!subCollectionMetaobjectDef) {
        throw new Error(
          '"Sub Collection Urls" metaobject definition not found'
        );
      }

      const styleField = subCollectionMetaobjectDef.fieldDefinitions.find(
        (field) => field.key === "style"
      );

      if (!styleField) {
        throw new Error(
          'Style field not found in "Sub Collection Urls" metaobject'
        );
      }

      // Get the current choices
      let currentChoicesForStylefield = [];

      const choicesValidationForStyle = styleField.validations.find(
        (v) => v.name === "choices"
      );

      if (choicesValidationForStyle && choicesValidationForStyle.value) {
        try {
          currentChoicesForStylefield = JSON.parse(
            choicesValidationForStyle.value
          );
        } catch (e) {
          console.warn(
            "Invalid choices JSON in style field, resetting to empty array."
          );
          currentChoicesForStylefield = [];
        }
      }

      // Replace the old name with the new style name in the choices array
      let updatedChoicesForMetaObjectStyleField =
        currentChoicesForStylefield.map((choice) =>
          choice === oldStyleName ? style_name : choice
        );

      updatedChoicesForMetaObjectStyleField = [
        ...new Set(updatedChoicesForMetaObjectStyleField),
      ];

      // Preserve other validations (excluding "choices")
      const otherValidations = styleField.validations.filter(
        (v) => v.name !== "choices"
      );

      // Add updated choices validation
      const updatedValidations = [
        ...otherValidations,
        {
          name: "choices",
          value: JSON.stringify(updatedChoicesForMetaObjectStyleField),
        },
      ];

      const UPDATE_METAOBJECT_DEFINITION = `
        mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
          metaobjectDefinitionUpdate(id: $id, definition: $definition) {
            metaobjectDefinition {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const definitionInput = {
        fieldDefinitions: {
          update: {
            key: "style",
            validations: updatedValidations,
          },
        },
      };

      const updateResponseForMetaObject = await graphqlClient(
        UPDATE_METAOBJECT_DEFINITION,
        {
          id: subCollectionMetaobjectDef.id,
          definition: definitionInput,
        }
      );

      const errors =
        updateResponseForMetaObject?.data?.metaobjectDefinitionUpdate
          ?.userErrors;

      if (errors && errors.length > 0) {
        throw new Error(`Failed to update field: ${JSON.stringify(errors)}`);
      } else {
        console.log(
          "Style field updated successfully in the predefined choices of collection urls definition."
        );
      }

      /**
       * Find the metaobject with the old style name and update it with the new style name.
       */
      // Now filteredMetaobjects contains all metaobjects with oldStyleName and type "Style"; Update the metaobjects.
      for (const metaobject of filteredMetaobjects) {
        const UPDATE_METAOBJECT_MUTATION = `
          mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
            metaobjectUpdate(id: $id, metaobject: $metaobject) {
              metaobject {
                id
                fields {
                  key
                  value
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const updateMetaobjectResponse = await graphqlClient(
          UPDATE_METAOBJECT_MUTATION,
          {
            id: metaobject.id,
            metaobject: {
              fields: [{ key: "name", value: style_name }],
            },
          }
        );

        if (
          updateMetaobjectResponse?.errors &&
          updateMetaobjectResponse.errors.length > 0
        ) {
          throw new Error(
            `GraphQL Errors while updating metaobject ${
              metaobject.id
            }: ${JSON.stringify(updateMetaobjectResponse.errors)}`
          );
        }

        const userErrors =
          updateMetaobjectResponse?.data?.metaobjectUpdate?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to update metaobject ${metaobject.id}: ${JSON.stringify(
              userErrors
            )}`
          );
        }

        console.log(`Metaobject ${metaobject.id} updated with new style name.`);
      }
    }

    /**
     *  Perform the image related operations
     */
    if (files.length == 0 && existing_url === "") {
      // if the user updates without image
      existing_url = null;

      // Perform the update in the shopify metaobjects; Remove the image if it exists;
      for (const metaobject of filteredMetaobjects) {
        const UPDATE_METAOBJECT_IMAGE_MUTATION = `
          mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
            metaobjectUpdate(id: $id, metaobject: $metaobject) {
              metaobject {
                id
                fields {
                  key
                  value
                  type
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const updateMetaobjectResponse = await graphqlClient(
          UPDATE_METAOBJECT_IMAGE_MUTATION,
          {
            id: metaobject.id,
            metaobject: {
              fields: [{ key: "image", value: "" }],
            },
          }
        );

        if (
          updateMetaobjectResponse?.errors &&
          updateMetaobjectResponse.errors.length > 0
        ) {
          throw new Error(
            `GraphQL Errors while removing image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(updateMetaobjectResponse.errors)}`
          );
        }

        const userErrors =
          updateMetaobjectResponse?.data?.metaobjectUpdate?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to remove image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(userErrors)}`
          );
        }

        console.log(`Metaobject ${metaobject.id} image field removed.`);
      }
    } else if (files.length > 0) {
      // Rename uploaded files
      files.forEach((file) => {
        const extension = file.originalname.split(".").pop();
        file.renamedTo = `${style_name}.${extension}`;
      });

      const file = files[0]; // Only use the first file

      // Step 1: Staged Upload Setup
      const stagedUploadsMutation = `
        mutation generateStagedUploadTarget($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters {
                name
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const uploadInput = [
        {
          filename: file.renamedTo,
          mimeType: file.mimetype,
          resource: "FILE",
          httpMethod: "POST",
        },
      ];

      const stagedUploadResponse = await graphqlClient(stagedUploadsMutation, {
        input: uploadInput,
      });

      const stagedTarget =
        stagedUploadResponse?.data?.stagedUploadsCreate?.stagedTargets?.[0];

      if (!stagedTarget) {
        throw new Error("Failed to get staged upload target from Shopify");
      }

      // Step 2: Upload file using axios + form-data
      const form = new FormData();
      for (const param of stagedTarget.parameters) {
        form.append(param.name, param.value);
      }
      form.append("file", file.buffer, file.renamedTo);

      await axios.post(stagedTarget.url, form, {
        headers: form.getHeaders(),
        maxBodyLength: Infinity,
      });

      file.uploadedUrl = stagedTarget.resourceUrl;

      // Step 3: Create File on Shopify
      const FILE_CREATE_MUTATION = `
        mutation fileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const filesCreateInput = [
        {
          originalSource: file.uploadedUrl,
          contentType: "IMAGE",
          alt: file.originalname,
        },
      ];

      const filesCreateResponse = await graphqlClient(FILE_CREATE_MUTATION, {
        files: filesCreateInput,
      });

      const createdFile = filesCreateResponse?.data?.fileCreate?.files?.[0];
      const fileId = createdFile?.id;

      // Wait for 3seconds for Shopify to index the file
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Step 5: Get permanent URL using ID-based file query
      const FILE_QUERY = `
        query getFile($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on MediaImage {
              id
              image {
                url
              }
            }
            ... on GenericFile {
              id
              url
            }
          }
        }
      `;

      const fileQueryResponse = await graphqlClient(FILE_QUERY, {
        ids: [fileId],
      });

      const fileNode = fileQueryResponse?.data?.nodes?.[0];

      let permanentUrl = null;

      if (fileNode?.image?.url) {
        permanentUrl = fileNode.image.url;
      } else if (fileNode?.url) {
        permanentUrl = fileNode.url;
      }

      if (!permanentUrl) {
        throw new Error(
          "Failed to fetch permanent URL for uploaded file from Shopify"
        );
      }

      file.uploadedUrl = permanentUrl;
      existing_url = permanentUrl;

      // Step 6: Update image field of all filtered metaobjects
      for (const metaobject of filteredMetaobjects) {
        const UPDATE_METAOBJECT_IMAGE_MUTATION = `
          mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
            metaobjectUpdate(id: $id, metaobject: $metaobject) {
              metaobject {
                id
                fields {
                  key
                  value
                  type
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const updateMetaobjectResponse = await graphqlClient(
          UPDATE_METAOBJECT_IMAGE_MUTATION,
          {
            id: metaobject.id,
            metaobject: {
              fields: [{ key: "image", value: fileId }],
            },
          }
        );

        if (updateMetaobjectResponse?.errors?.length > 0) {
          throw new Error(
            `GraphQL Errors while updating image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(updateMetaobjectResponse.errors)}`
          );
        }

        const userErrors =
          updateMetaobjectResponse?.data?.metaobjectUpdate?.userErrors;
        if (userErrors?.length > 0) {
          throw new Error(
            `Failed to update image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(userErrors)}`
          );
        }

        console.log(`Metaobject ${metaobject.id} image field was updated.`);
      }
    }

    /**
     *  Perform the final update in the stlr_styles table in the DB
     */
    await connection.query(
      "UPDATE stlr_styles SET style_name = ?, image = ? WHERE style_id = ?",
      [style_name, existing_url, numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Style updated successfully.",
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  } finally {
    connection.release();
  }
};

const deleteSingleStyle = async (req, res) => {
  const deleteStyleSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Style ID must be a number.",
      "number.integer": "Style ID must be an integer.",
      "number.positive": "Style ID must be a positive number.",
      "any.required": "Style ID is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = deleteStyleSchema.validate({ id: numericId });

  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if style exists
    const [styles] = await connection.query(
      "SELECT * FROM stlr_styles WHERE style_id = ? LIMIT 1",
      [numericId]
    );

    if (styles.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Style not found. Please provide a valid style ID.",
      });
    }

    const styleName = styles[0].style_name;

    /**
     *  Check whether any product has this style name or not. If any product has this style name, then return error.
     */
    const PRODUCT_WITH_STYLE_NAME_QUERY = `
      query {
        products(first: 100) {
          edges {
            node {
              id
              title
              metafields(first: 10, namespace: "custom") {
                edges {
                  node {
                    key
                    value
                  }
                }
              }
            }
          }
        }
      }
    `;

    const productCheckResponse = await graphqlClient(
      PRODUCT_WITH_STYLE_NAME_QUERY
    );
    const productEdges = productCheckResponse?.data?.products?.edges || [];

    const usedProduct = productEdges.find((edge) => {
      const metafields = edge.node.metafields?.edges?.map((m) => m.node) || [];
      return metafields.some(
        (mf) => mf.key === "style" && mf.value.trim() === styleName.trim()
      );
    });

    if (usedProduct) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Cannot delete style '${styleName}' because it is used in one or more products.`,
      });
    }

    /**
     *  If there is no product with this style name, then delete the style name from the predefined metafield validations.
     */
    // Get the Style Name metafield definition
    const METAFIELD_DEFINITIONS_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );
    const styleNameMetafield = definitions.find((def) => def.name === "Style");

    if (!styleNameMetafield) {
      throw new Error("Style Name metafield definition not found");
    }

    const styleNameMetafieldId = styleNameMetafield.id;

    // Get the existing style names from the metafield "validations"
    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${styleNameMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
    const validations = definition?.validations || [];

    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        currentChoices = [];
      }
    }

    // Remove the deleted style name from the choices array
    const updatedChoices = currentChoices.filter(
      (choice) => choice !== styleName
    );

    // Update the metafield definition with the new list of style names
    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: styleNameMetafield.namespace,
        key: styleNameMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(updatedChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    if (updateResponse?.errors) {
      throw new Error("GraphQL mutation failed");
    }

    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

    if (userErrors && userErrors.length > 0) {
      throw new Error("Failed to update Shopify style list");
    }

    /**
     * Delete the predefined choices for the field "Style" in the metaobject definition "Sub Collection Urls".
     */
    const GET_METAOBJECT_DEFINITIONS = `
      query {
        metaobjectDefinitions(first: 100) {
          edges {
            node {
              id
              name
              fieldDefinitions {
                key
                name
                type {
                  name
                }
                required
                validations {
                  name
                  value
                }
              }
            }
          }
        }
      }
    `;

    // Fetch all metaobject definitions
    const metaobjectDefsResponse = await graphqlClient(
      GET_METAOBJECT_DEFINITIONS
    );

    const metaobjectDefs =
      metaobjectDefsResponse?.data?.metaobjectDefinitions?.edges?.map(
        (edge) => edge.node
      );

    if (!metaobjectDefs || metaobjectDefs.length === 0) {
      throw new Error("No metaobject definitions found");
    }

    // Find the "Sub Collection Urls" metaobject definition by name
    const subCollectionMetaobjectDef = metaobjectDefs.find(
      (def) => def.name === "Sub Collection Urls"
    );

    if (!subCollectionMetaobjectDef) {
      throw new Error('"Sub Collection Urls" metaobject definition not found');
    }

    const styleField = subCollectionMetaobjectDef.fieldDefinitions.find(
      (field) => field.key === "style"
    );

    if (!styleField) {
      throw new Error(
        'Style field not found in "Sub Collection Urls" metaobject'
      );
    }

    let currentChoicesForStylefield = [];

    const choicesValidationForStyle = styleField.validations.find(
      (v) => v.name === "choices"
    );

    if (choicesValidationForStyle && choicesValidationForStyle.value) {
      try {
        currentChoicesForStylefield = JSON.parse(
          choicesValidationForStyle.value
        );
      } catch (e) {
        currentChoicesForStylefield = [];
      }
    }

    // Remove the style_name from the choices array
    currentChoicesForStylefield = currentChoicesForStylefield.filter(
      (choice) => choice !== styleName
    );

    // Preserve other validations (excluding "choices")
    const otherValidations = styleField.validations.filter(
      (v) => v.name !== "choices"
    );

    // Add updated choices validation
    const updatedValidations = [
      ...otherValidations,
      {
        name: "choices",
        value: JSON.stringify(currentChoicesForStylefield),
      },
    ];

    const UPDATE_METAOBJECT_DEFINITION = `
      mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
        metaobjectDefinitionUpdate(id: $id, definition: $definition) {
          metaobjectDefinition {
            id
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const definitionInput = {
      fieldDefinitions: {
        update: {
          key: "style",
          validations: updatedValidations,
        },
      },
    };

    const updateResponseForMetaObject = await graphqlClient(
      UPDATE_METAOBJECT_DEFINITION,
      {
        id: subCollectionMetaobjectDef.id,
        definition: definitionInput,
      }
    );

    const errors =
      updateResponseForMetaObject?.data?.metaobjectDefinitionUpdate?.userErrors;

    if (errors && errors.length > 0) {
      throw new Error(`Failed to update field: ${JSON.stringify(errors)}`);
    } else {
      console.log("Style field updated successfully.");
    }

    /**
     * Delete the metaobject with this style name in the definition "filter_images".
     */
    // Get all metaobjects with the old style name
    const METAOBJECTS_BY_TYPE_QUERY = `
        query MetaobjectsByType($type: String!) {
          metaobjects(first: 100, type: $type) {
            edges {
              node {
                id
                type
                fields {
                  key
                  value
                }
              }
            }
          }
        }
    `;

    const metaobjectsResponse = await graphqlClient(METAOBJECTS_BY_TYPE_QUERY, {
      type: "filter_images",
    });

    const metaobjects =
      metaobjectsResponse?.data?.metaobjects?.edges?.map((edge) => edge.node) ||
      [];

    // Filter metaobjects whose "name" field matches oldStyleName and "type" field is "Style"
    const filteredMetaobjects = metaobjects.filter((metaobject) => {
      if (!Array.isArray(metaobject.fields)) return false;
      const nameField = metaobject.fields.find((f) => f.key === "name");
      const typeField = metaobject.fields.find((f) => f.key === "type");
      return (
        nameField &&
        typeof nameField.value === "string" &&
        nameField.value.trim() === styleName.trim() &&
        typeField &&
        typeof typeField.value === "string" &&
        typeField.value.trim() === "Style"
      );
    });

    // Perform the deletion of the metaobjects if there are any filtered results
    if (filteredMetaobjects.length > 0) {
      const DELETE_METAOBJECT_MUTATION = `
        mutation DeleteMetaobject($id: ID!) {
          metaobjectDelete(id: $id) {
            deletedId
            userErrors {
              field
              message
            }
          }
        }
      `;

      for (const metaobject of filteredMetaobjects) {
        const deleteResponse = await graphqlClient(DELETE_METAOBJECT_MUTATION, {
          id: metaobject.id,
        });

        if (
          deleteResponse?.errors &&
          deleteResponse.errors.length > 0 &&
          deleteResponse.errors[0].message !== "Metaobject not found"
        ) {
          throw new Error(
            `Failed to delete metaobject ${metaobject.id}: ${JSON.stringify(
              deleteResponse.errors
            )}`
          );
        }

        const userErrors = deleteResponse?.data?.metaobjectDelete?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to delete metaobject ${metaobject.id}: ${JSON.stringify(
              userErrors
            )}`
          );
        }

        console.log(`Metaobject ${metaobject.id} deleted successfully.`);
      }
    }

    /**
     *  Update the table stlr_styles.
     */
    await connection.execute(
      `UPDATE wedding_band.stlr_rings SET style_id = NULL WHERE style_id = ?`,
      [numericId]
    );

    await connection.execute(
      `DELETE FROM wedding_band.stlr_styles WHERE style_id = ?`,
      [numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Style deleted successfully.",
    });
  } catch (error) {
    await connection.rollback();
    console.error("Delete style error:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

export {
  getStyle,
  createStyle,
  getStylesByIds,
  updateSingleStyle,
  deleteSingleStyle,
};
