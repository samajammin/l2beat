import { CONTRACTS, NEW_CRYPTOGRAPHY, RISK_VIEW } from './common'
import { Project } from './types'
import { zkswap } from './zkswap'

export const zkspace: Project = {
  name: 'ZKSpace',
  slug: 'zkspace',
  associatedTokens: ['ZKS'],
  bridges: [
    {
      address: '0x6dE5bDC580f55Bc9dAcaFCB67b91674040A247e3',
      sinceBlock: 12810001,
      tokens: '*',
    },
  ],
  details: {
    description:
      'The ZKSpace platform consists of three main parts: a Layer 2 AMM DEX utilizing ZK-Rollups technology called ZKSwap, a payment service called ZKSquare, and an NFT marketplace called ZKSea.',
    purpose: 'Payments, Exchange',
    links: {
      websites: ['https://zks.org/'],
      apps: ['https://zks.app'],
      documentation: ['https://en.wiki.zks.org/'],
      explorers: ['https://zkswap.info'],
      repositories: ['https://github.com/l2labs/zkswap-contracts'],
      socialMedia: [
        'https://medium.com/@zkspaceofficial',
        'https://twitter.com/ZKSpaceOfficial',
        'https://discord.gg/UbjmQfUVvf',
        'https://t.me/ZKSpaceOfficial',
        'https://reddit.com/r/ZKSwap_Official/',
      ],
    },
    riskView: {
      stateValidation: RISK_VIEW.STATE_ZKP_SN,
      dataAvailability: RISK_VIEW.DATA_ON_CHAIN,
      upgradeability: RISK_VIEW.UPGRADE_DELAY('8 days'),
      sequencerFailure: RISK_VIEW.SEQUENCER_FORCE_EXIT_L1,
      validatorFailure: RISK_VIEW.VALIDATOR_ESCAPE_ZKP,
    },
    technology: {
      category: zkswap.details.technology.category,
      stateCorrectness: zkswap.details.technology.stateCorrectness,
      newCryptography: {
        ...NEW_CRYPTOGRAPHY.ZK_SNARKS,
        references: [
          {
            text: 'ZKSpace Whitepaper',
            href: 'https://github.com/l2labs/zkspace-whitepaper',
          },
        ],
      },
      dataAvailability: zkswap.details.technology.dataAvailability,
      operator: zkswap.details.technology.operator,
      forceTransactions: zkswap.details.technology.forceTransactions,
      exitMechanisms: zkswap.details.technology.exitMechanisms,
      contracts: {
        addresses: [
          {
            address: '0x0DCCe462ddEA102D3ecf84A991d3ecFC251e02C7',
            name: 'UpgradeGatekeeper',
            description:
              'This is most likely the contract that implements the upgrade mechanism for Governance and ZkSync. It relies on the ZkSync contract to enforce upgrade delays. The source code of this contract is not verified on Etherscan.',
          },
          {
            address: '0x6dE5bDC580f55Bc9dAcaFCB67b91674040A247e3',
            name: 'ZkSync',
            description:
              'This contract defines the upgrade delay. Unfortunately this information is stored in an internal constant and not exposed as a public view method. The UPGRADE_NOTICE_PERIOD constant is currently set to 691200 seconds which equals 8 days. Every time the contract upgrades this information has to be verified again.',
            upgradeability: {
              type: 'EIP1967',
              implementation: '0xf2c351f22b148A9fF583a0F81701471a74E7338e',
              admin: '0x0DCCe462ddEA102D3ecf84A991d3ecFC251e02C7',
            },
          },
          {
            address: '0x86E527BC3C43E6Ba3eFf3A8CAd54A7Ed09cD8E8B',
            name: 'Governance',
            upgradeability: {
              type: 'EIP1967',
              implementation: '0x95269f9E76540459c797089034dc74b48dF780a2',
              admin: '0x0DCCe462ddEA102D3ecf84A991d3ecFC251e02C7',
            },
          },
          {
            address: '0x94b9401945a9bc06CE5B69e6dB3c6B671aABc829',
            name: 'Verifier',
          },
          {
            address: '0xC0221a4Dfb792AA71CE84C2687b1D2b1E7D3eea0',
            name: 'ZkSyncExit',
            description: CONTRACTS.UNVERIFIED_DESCRIPTION,
            upgradeability: {
              type: 'Reference',
              base: 'ZkSync',
              method: 'function zkSyncExitAddress() view returns(address)',
            },
          },
          {
            address: '0xE26Ebb18144CD2d8DCB14cE87fdCfbEb81baCAD4',
            name: 'ZkSyncCommitBlock',
            upgradeability: {
              type: 'Reference',
              base: 'ZkSync',
              method:
                'function zkSyncCommitBlockAddress() view returns(address)',
            },
          },
        ],
        risks: [
          CONTRACTS.UPGRADE_WITH_DELAY_RISK('8 days'),
          CONTRACTS.UNVERIFIED_RISK,
        ],
      },
    },
    news: zkswap.details.news,
  },
}
