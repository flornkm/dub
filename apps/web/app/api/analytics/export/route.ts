import { getAnalytics } from "@/lib/analytics";
import { DubApiError } from "@/lib/api/errors";
import { withAuth } from "@/lib/auth";
import { getDomainViaEdge } from "@/lib/planetscale";
import prisma from "@/lib/prisma";
import { getAnalyticsQuerySchema } from "@/lib/zod/schemas/analytics";
import { linkConstructor } from "@dub/utils";
import { json2csv } from "json-2-csv";
import JSZip from "jszip";

const convertToCSV = (data) => {
  return json2csv(data, {
    parseValue(fieldValue, defaultParser) {
      if (fieldValue instanceof Date) {
        return fieldValue.toISOString();
      }
      return defaultParser(fieldValue);
    },
  });
};

const exportableEndpoints = [
  "timeseries",
  "country",
  "top_urls",
  "device",
  "referer",
  "city",
  "browser",
  "os",
  "top_links",
];

export const GET = withAuth(
  async ({ searchParams, workspace, link }) => {
    const parsedParams = getAnalyticsQuerySchema.parse(searchParams);
    const { domain, key, interval } = parsedParams;

    if (
      workspace?.plan === "free" &&
      (interval === "all" || interval === "90d")
    ) {
      throw new DubApiError({
        code: "forbidden",
        message: "Require higher plan",
      });
    }

    const linkId = link
      ? link.id
      : domain && key === "_root"
        ? await getDomainViaEdge(domain).then((d) => d?.id)
        : null;

    const zip = new JSZip();
    const promises = [] as Promise<void>[];

    for (const endpoint of exportableEndpoints) {
      let promise: Promise<void>;

      if (endpoint === "top_links") {
        promise = getTopLinksData(workspace, parsedParams, zip);
      } else {
        promise = getAnalyticsData(
          workspace,
          linkId,
          endpoint,
          parsedParams,
          zip,
        );
      }
      promises.push(promise);
    }

    await Promise.all(promises);

    const zipData = await zip.generateAsync({ type: "nodebuffer" });

    return new Response(zipData, {
      headers: {
        "Content-Type": "application/zip",
        "Content-Disposition": "attachment; filename=analytics_export.zip",
      },
    });
  },
  {
    needNotExceededClicks: true,
  },
);

async function getTopLinksData(workspace, parsedParams, zip) {
  const data = await getAnalytics({
    workspaceId: workspace.id,
    endpoint: "top_links",
    ...parsedParams,
  });

  if (!data || data.length === 0) return;

  const [links, domains] = await Promise.all([
    prisma.link.findMany({
      where: {
        projectId: workspace.id,
        id: { in: data.map(({ link }) => link) },
      },
      select: { id: true, domain: true, key: true, url: true },
    }),
    prisma.domain.findMany({
      where: {
        projectId: workspace.id,
        id: { in: data.map(({ link }) => link) },
      },
      select: { id: true, slug: true, target: true },
    }),
  ]);

  const allLinks = [
    ...links.map((link) => ({
      linkId: link.id,
      shortLink: linkConstructor({
        domain: link.domain,
        key: link.key,
        pretty: true,
      }),
      url: link.url,
    })),
    ...domains.map((domain) => ({
      linkId: domain.id,
      shortLink: linkConstructor({
        domain: domain.slug,
        pretty: true,
      }),
      url: domain.target || "",
    })),
  ];

  const topLinks = data.map((d) => ({
    ...allLinks.find((l) => l.linkId === d.link),
    clicks: d.clicks,
  }));

  const csvData = convertToCSV(topLinks);
  zip.file(`top_links.csv`, csvData);
}

async function getAnalyticsData(
  workspace,
  linkId,
  endpoint,
  parsedParams,
  zip,
) {
  const response = await getAnalytics({
    workspaceId: workspace.id,
    ...(linkId && { linkId }),
    endpoint,
    ...parsedParams,
  });

  if (!response || response.length === 0) return;

  const csvData = convertToCSV(response);
  zip.file(`${endpoint}.csv`, csvData);
}
